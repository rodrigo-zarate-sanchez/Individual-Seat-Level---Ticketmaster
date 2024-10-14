# Individual-Seat-Level---Ticketmaster
Code for expanding the seat blocks from Ticketmaster to the individual seat level

Expanding Ticketmaster Seat Blocks to Individual Seat Level

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
