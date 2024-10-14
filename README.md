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
