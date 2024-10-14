# Expanding the Seat Block to the Individual Seat Level in Ticketmaster

The following article aims to propose a coding solution for expanding the seat block from Ticketmaster to the individual seat level.

## Context

Ticketmaster uses blocks of seats and not individual seats. For example, if a customer purchases seats 1 – 4, the system treats this as a single transaction line item. This transaction line item will show the first seat of the block as 1 and the number of seats (num_seats) as 4, indicating that the customer purchased 4 seats. In other words, the customer doesn't have seat 1, 2, 3, and 4 separately, but seats 1-4 as a block.

This particularity can be problematic due to the way seat comparisons need to be made. Ticketmaster explains this as follows: 

> “When an entry comes over in the incremental export, the `event_name`, `section_name`, `row_name`, `first_seat`, `last_seat`, and `add_datetime` should be used for comparison. A one-to-one comparison cannot be used as our system uses seat blocks, and these seat blocks can be split and re-joined. The comparison needs to check if that seat already exists in the table. If it exists, the incoming data needs to replace the existing seat data.”

To solve this problem, I am sharing a code below that expands the block of seats to the individual seat level. This solution is based on the use of a Common Table Expression (CTE) and then, through hierarchy and record connection, the code breaks the block of seats down into individual seats.

Before describing the code, here is a brief explanation of what a CTE is.

## What is a CTE (Common Table Expression)?

A CTE (Common Table Expression) is a temporary result set that you can reference within a `SELECT`, `INSERT`, `UPDATE`, or `DELETE` statement. It's often used to simplify complex queries by breaking them into more manageable subqueries. In SQL, CTEs are defined using the `WITH` keyword, followed by a query definition.

A CTE is similar to creating a temporary table, but it only exists during the execution of the query and is not stored in the database. One of the main advantages of CTEs is that they make queries easier to read and maintain, especially when dealing with recursive operations or when the same subquery needs to be referenced multiple times in a larger query.

### Syntax Example

```sql
WITH CTE_name AS (
    SELECT columns
    FROM table_name
    WHERE conditions
)
SELECT *
FROM CTE_name;
```

### Proposed Code
Below is part of the code. The objective is to present a macro solution, so the code is not exhaustive. If you have any questions, please feel free to reach out.

This query assumes that the data comes from the TICKETI_STAGING table (where incrementals are added to the initial full load). The goal is to create a table named SEATBYSEAT_F, where each row represents an individual seat.

It’s important to adjust the revenue values (e.g., block purchase price, paid amount, and owed amount) to the individual seat level, as these values are assigned to the block of seats. See the specific code for these fields below.

```sql
-- Create a table named SEATBYSEAT_test using a query that generates seat-level ticket data
CREATE TABLE SEATBYSEAT_F AS
-- Define a Common Table Expression (CTE) named 'seat_series' to build seat-specific data
WITH seat_series AS (
    SELECT 
        ts.EVENT_NAME,            -- Event name associated with the ticket
        ts.SECTION_NAME,          -- Section name within the event
        ts.ROW_NAME,              -- Row name within the section
        ts.ROW_ID,                -- Row identifier
        ts.ACCT_ID,               -- Account ID of the ticket holder
        ts.TICKET_STATUS,         -- Ticket status (e.g., active, reserved)
        (ts.SEAT_NUM + LEVEL - 1) AS SEAT, -- Generate seat numbers dynamically based on hierarchical level
        ts.EVENT_NAME || '-' || ts.SECTION_NAME || '-' || ts.ROW_NAME || '-' || (ts.SEAT_NUM + LEVEL - 1) AS KEY,  -- Create a unique key for each seat using event, section, row, and seat number
        ts.SECTION_NAME || '-' || ts.ROW_NAME || '-' || (ts.SEAT_NUM + LEVEL - 1) AS KEY_SEAT, 
        ts.BLOCK_PURCHASE_PRICE / (ts.LAST_SEAT - ts.SEAT_NUM + 1) AS PURCHASE_PRICE,  -- Calculate the purchase price per seat by dividing the total block purchase price by the number of seats
        ts.PAID_AMOUNT / (ts.LAST_SEAT - ts.SEAT_NUM + 1) AS PAID_AMOUNT,           -- Paid amount from TICKETI_STAGING
        ts.OWED_AMOUNT / (ts.LAST_SEAT - ts.SEAT_NUM + 1) AS OWED_AMOUNT,           -- Owed amount from TICKETI_STAGING
        -- Find the maximum TICKET_SEQ_ID for each unique seat key to identify the most recent record
        MAX(ts.TICKET_SEQ_ID) OVER (PARTITION BY ts.EVENT_NAME || '-' || ts.SECTION_NAME || ts.ROW_NAME || '-' || (ts.SEAT_NUM + LEVEL - 1)) AS max_TICKET_SEQ_ID,

        -- Assign row numbers within partitions based on event, section, row, and seat number
        ROW_NUMBER() OVER (
            PARTITION BY ts.EVENT_NAME, ts.SECTION_NAME, ts.ROW_NAME, 
            ts.TICKET_SEQ_ID, 
            (ts.SEAT_NUM + LEVEL - 1) 
            ORDER BY 
            ts.EVENT_NAME ASC,            -- Order by event name ascending
            ts.SECTION_NAME ASC,          -- Order by section name ascending
            ts.ROW_ID ASC,                -- Order by row ID ascending
            ts.UPD_DATETIME DESC,         -- Order by update date descending (latest first)
            ts.ORDER_NUM DESC,            -- Order by order number descending
            ts.TICKET_SEQ_ID DESC,        -- Order by ticket sequence ID descending
            ts.TICKET_STATUS DESC,        -- Order by ticket status descending
            ts.SEQ_ID ASC                 -- Order by sequence ID ascending
        ) AS rn  -- Row number to identify unique records
    FROM 
        TICKETI_STAGING ts   -- Source table with ticket data
    WHERE 
        ts.LAST_SEAT >= ts.SEAT_NUM  -- Filter for valid seat ranges (last seat is greater than or equal to the first)
        AND ts.TICKET_STATUS IN ('A', 'R')  -- Consider only tickets with 'A' (Active) or 'R' (Reserved) status
    CONNECT BY 
        -- Hierarchical query to generate seat numbers for each row based on the seat range
        LEVEL <= (ts.LAST_SEAT - ts.SEAT_NUM + 1)
    AND 
        -- Ensures no invalid recursive loops by randomizing the join condition
        PRIOR ts.TICKET_SEQ_ID = ts.TICKET_SEQ_ID 
    AND 
        PRIOR dbms_random.value IS NOT NULL  -- Prevent infinite loops with random values
    AND 
        PRIOR ts.SEQ_ID = ts.SEQ_ID  -- Ensure the same sequence ID for the hierarchy
)

