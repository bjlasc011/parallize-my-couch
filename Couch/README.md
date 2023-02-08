# Utilities
- update the variables at the top of the main method to connect to couchbase.
- to observe how Tasks are queued differently change the different variable enums or degree of parallelism.

- run the Couch proj in debug mode.
- this will pause after bootstrapping the couchbase cluster. 
- Optionally > 
	- cd into the dir containing the Couch.csproj 
	- run `counters.bat <process-id>` this process Id is the process of the VisualStudio debugger and can be seen in the top left corner
	of VS when running in debug mode.
	- this opens a counters window that displays thread pool counter values. We want a high number of completed tasks, low numbers for contention,
	and low numbers for thread count and queued tasks. 
	- When thread starvation occurs you will see thread count slowly climb, completed tasks drop dramatically, and queued items rapidly increase.