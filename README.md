############

Queue Worker 

############

Description:
This library allows the caller to schedule worker at a specific time.
The worker would execute any tasks in the past.  

API:
Initialize() - initialize the queue
Close() - shutdown the queue
AddTask() - Adds task into the queue
RunSchedule() - starts the daemon

Model:
Task

Notes:
- I was able to find a ORM for postgresql.  I tried structuring the code to 
use models.  
- Ideally, I would use transaction, so the states are consistent.  However, I did not have the cycles to correctly implement the transaction functionality of this library.  Also, locking does have its drawback with performance, and hinders parallelism.  Therefore, I will follow the AWS SQS model, where there is a small chance multiple workers executing the same task.
- Please note I have fake FAILUREs, so I can simulate updating different states on the database.

=== SETUP (MACOS) ===
1. Clone repo
git clone git@github.com:ecnerwalawrence/nerdwallet.git

2. Install Go
https://golang.org/dl/

3. Set ENV variables
GO111MODULE=on

4. cd into repo

5. Setup Postgres
Note, if you change the name, you need to fix the code.
DatabaseName: 
nerdwallet_ecnerwal 
DatabaseUser:
postgres

6. Initialize Schema
psql nerdwallet_ecnerwal < schema.sql

=== RUN ===

1. Run unit test
go test -v ./...

2. Run code (Please note this runs both AddTask and RunSchedule together)
go run -v main.go