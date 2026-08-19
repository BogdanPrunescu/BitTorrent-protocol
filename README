**Prunescu Bogdan-Andrei — 331CA**

For this assignment, I followed the general workflow and explanations provided in the statement.

1. For each client, I open and read its input file in order to obtain information about the files it already owns and the files it wants to download.

2. Each client sends the tracker information about the file segments it currently owns. After receiving initial information from all clients, the tracker sends a confirmation message so clients can start the download phase.

3. The tracker then waits for messages from clients. The type of each request is identified using an integer value. When a client wants to download a file, it first sends a `FILE_REQUEST` message to the tracker. The tracker responds with information about the file segments and the clients that currently own them.

4. After receiving this information, the client starts downloading the required segments. For each segment, it randomly selects one of the available seeds that owns that segment and sends a request to it. After every 10 segments are downloaded, the client sends an `UPDATE_CLIENT` message to the tracker with its updated list of owned segments. It also sends another `FILE_REQUEST` in order to receive the updated information about the available segments and their owners.

5. When a client finishes downloading a file, the tracker marks it as a seed for that file. The tracker also records how many clients have finished downloading all their requested files. When all clients are finished, the tracker sends a termination message to every client so that their upload threads can stop. The download thread finishes automatically after the client has downloaded all the files it requested.
