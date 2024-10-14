# Individual-Seat-Level---Ticketmaster
Code for expanding the seat blocks from Ticketmaster to the individual seat level

Expanding Ticketmaster Seat Blocks to Individual Seat Level

Objective
This article aims to propose a coding solution for expanding Ticketmaster seat blocks to the individual seat level.

Context
Ticketmaster operates using seat blocks rather than individual seat assignments. For instance, if a customer buys seats 1 to 4, the system treats this as one transaction line item. It will indicate the first seat in the block (seat 1) and the number of seats (4), meaning the customer has purchased four seats within that block. In essence, the system doesn’t specify that the customer owns seats 1, 2, 3, and 4, but instead a block from seat 1 to seat 4.

This can present challenges when seat-level comparisons are required. Ticketmaster elaborates: "When an entry is included in the incremental export, fields such as event_name, section_name, row_name, first_seat, last_seat, and add_datetime must be used for comparison. A one-to-one comparison cannot be performed since our system uses seat blocks, which can be split and rejoined. The comparison must check whether the seat already exists in the table. If it does, the new data should replace the existing seat data."

To address this issue, below is a proposed solution that expands the block of seats into individual seat records. This approach uses a CTE (Common Table Expression) combined with hierarchical queries to break down seat blocks into individual seat-level data.


What is a CTE (Common Table Expression)?
A CTE is a temporary result set that can be referenced within SQL statements like SELECT, INSERT, UPDATE, or DELETE. CTEs are particularly useful for simplifying complex queries, especially when recursive operations are needed or when the same subquery is reused multiple times within a larger query.

A CTE is similar to a temporary table, but it only exists for the duration of the query execution. CTEs are helpful in improving query readability and maintainability.

Example Syntax for CTE:
sql
Copy code
WITH CTE_name AS (
    SELECT columns
    FROM table_name
    WHERE conditions
)
SELECT *
FROM CTE_name;
Proposed Solution
The following is a sample of the proposed code. It is a macro-level solution designed to give a conceptual understanding, and the code is not exhaustive. Feel free to reach out if you have any questions.

This query assumes the data originates from the table TICKETI_STAGING, where incremental data is added to the initial full load. The goal is to create a table named SEATBYSEAT_F where each row represents a distinct seat.

A key point to highlight is that revenue values, such as the total block purchase price and the paid/owed amounts, must be adjusted to reflect the individual seat level, as they are currently assigned to the seat block. Specific adjustments for these fields are detailed in the code below.

SQL Query
sql
Copy code
-- Create a table named SEATBYSEAT_F using a query that generates seat-level ticket data
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
        (ts.SEAT_NUM + LEVEL - 1) AS SEAT,  -- Generate seat numbers dynamically based on hierarchical level
        ts.EVENT_NAME || '-' || ts.SECTION_NAME || '-' || ts.ROW_NAME || '-' || (ts.SEAT_NUM + LEVEL - 1) AS KEY,  -- Create a unique key for each seat using event, section, row, and seat number
        ts.BLOCK_PURCHASE_PRICE / (ts.LAST_SEAT - ts.SEAT_NUM + 1) AS PURCHASE_PRICE,  -- Calculate the purchase price per seat by dividing the total block purchase price by the number of seats
        ts.PAID_AMOUNT / (ts.LAST_SEAT - ts.SEAT_NUM + 1) AS PAID_AMOUNT,  -- Paid amount adjusted per seat
        ts.OWED_AMOUNT / (ts.LAST_SEAT - ts.SEAT_NUM + 1) AS OWED_AMOUNT,  -- Owed amount adjusted per seat
        -- Find the maximum TICKET_SEQ_ID for each unique seat key to identify the most recent record
        MAX(ts.TICKET_SEQ_ID) OVER (PARTITION BY ts.EVENT_NAME || '-' || ts.SECTION_NAME || '-' || ts.ROW_NAME || '-' || (ts.SEAT_NUM + LEVEL - 1)) AS max_TICKET_SEQ_ID,
        -- Assign row numbers within partitions based on event, section, row, and seat number
        ROW_NUMBER() OVER (
            PARTITION BY ts.EVENT_NAME, ts.SECTION_NAME, ts.ROW_NAME, 
            ts.TICKET_SEQ_ID, 
            (ts.SEAT_NUM + LEVEL - 1) 
            ORDER BY 
            ts.UPD_DATETIME DESC  -- Order by update date descending
        ) AS rn  -- Row number to identify unique records
    FROM 
        TICKETI_STAGING ts  -- Source table with ticket data
    WHERE 
        ts.LAST_SEAT >= ts.SEAT_NUM  -- Filter for valid seat ranges
        AND ts.TICKET_STATUS IN ('A', 'R')  -- Consider only active or reserved tickets
    CONNECT BY 
        LEVEL <= (ts.LAST_SEAT - ts.SEAT_NUM + 1)  -- Hierarchical query to generate seat numbers for each row
)
This solution efficiently converts seat blocks into individual seat records, making the comparison and analysis of seat-level data more straightforward. The query uses Oracle SQL's CONNECT BY clause to dynamically generate seat numbers from seat blocks.
