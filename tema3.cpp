#include <mpi.h>
#include <pthread.h>
#include <bits/stdc++.h>

#define TRACKER_RANK 0
#define MAX_FILES 10
#define MAX_FILENAME 15
#define HASH_SIZE 32
#define MAX_CHUNKS 100

#define ACK_TAG 10
#define OP_TAG 20
#define REQ_TAG 30

#define NO_MESSAGE 0
#define FILE_REQUEST 10
#define UPDATE_CLIENT 11
#define FINISH_ONE 12
#define FINISH_ALL 13

using namespace std;

int recv1i(int source) {
    int buf;
    MPI_Recv(&buf, 1, MPI_INT, source, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    return buf;
}

string recvhash(int source) {
    char hash[HASH_SIZE + 1];
    MPI_Recv(hash, HASH_SIZE + 1, MPI_CHAR, source, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    return hash;
}

string recvfilename(int source) {
    char file_name[MAX_FILENAME + 1];
    MPI_Recv(file_name, MAX_FILENAME + 1, MPI_CHAR, source, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    return file_name;
}

int recv_ack(int source) {
    int ack;
    MPI_Recv(&ack, 1, MPI_INT, source, ACK_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    return ack;
}

vector<int> recvlisti(int source) {
    int buf[MAX_CHUNKS + 1];
    vector<int> my_vec;
    int size = recv1i(source);
    MPI_Recv(buf, size, MPI_INT, source, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    for (int i = 0; i < size; i++) {
        my_vec.push_back(buf[i]);
    }
    return my_vec;
}

void send1i(int dest, int *buf) {
    MPI_Send(buf, 1, MPI_INT, dest, 0, MPI_COMM_WORLD);
}

void sendhash(int dest, char* buf) {
    MPI_Send(buf, HASH_SIZE + 1, MPI_CHAR, dest, 0, MPI_COMM_WORLD);
}

void sendfilename(int dest, char *buf) {
    MPI_Send(buf, MAX_FILENAME + 1, MPI_CHAR, dest, 0, MPI_COMM_WORLD);
}

void send_ack_ok(int dest) {
    int ack = 200;
    MPI_Send(&ack, 1, MPI_INT, dest, ACK_TAG, MPI_COMM_WORLD);
}

void sendlisti(int dest, int *buf, int size) {
    send1i(dest, &size);
    MPI_Send(buf, size, MPI_INT, dest, 0, MPI_COMM_WORLD);
}

unordered_map<string, vector<string>> files;


vector<string> needed_files;
unordered_map<string, vector<pair<bool, string>>> downloads;

/** Function that updates segment owners for a file
 *  @param need_hash used to also store the hashes for the file
 *  and is used the first time a file is requested
 */
int file_request(string file_name, vector<set<int>> &segm_peers, bool need_hash) {
    int msg_type = FILE_REQUEST;
    MPI_Send(&msg_type, 1, MPI_INT, TRACKER_RANK, OP_TAG, MPI_COMM_WORLD);

    sendfilename(TRACKER_RANK, file_name.data());
    int file_size = recv1i(TRACKER_RANK);

    for (int i = 0; i < file_size; i++) {
        string hash = recvhash(TRACKER_RANK);
        vector<int> peers = recvlisti(TRACKER_RANK);
        set<int> peers_set;
        for (auto peer : peers) {
            peers_set.insert(peer);
        }
        segm_peers.push_back(peers_set);
        if (need_hash) {
            downloads[file_name].push_back({false, hash});
        }
    }
    
    return file_size;
}

/** Function that updates the tracker with the newly downloaded segments
 *  @param idx_start index of first hash newly downloaded
 *  @param hash_count number of hashes downloaded
 */
void update_client(string file_name, int idx_start, int hash_count) {
    int msg_type = UPDATE_CLIENT;
    MPI_Send(&msg_type, 1, MPI_INT, TRACKER_RANK, OP_TAG, MPI_COMM_WORLD);

    sendfilename(TRACKER_RANK, file_name.data());
    send1i(TRACKER_RANK, &hash_count);
    send1i(TRACKER_RANK, &idx_start);
}

void *download_thread_func(void *arg)
{
    int rank = *(int*) arg;

    for (auto file_name : needed_files) {
        // data structure used to store peers for hashes
        // segm_peers[i] -> peers for segment i
        vector<set<int>> segm_peers;

        int file_size = file_request(file_name, segm_peers, true);
        
        for (int i = 0; i < file_size; i++) {
            if (i % 10 == 0 && i != 0) {
                file_request(file_name, segm_peers, false);

                update_client(file_name, i - 10, 10);
            }
            // get a random peer/seed
            int select_peer = rand() % segm_peers[i].size();
            int peer_id = *next(segm_peers[i].begin(), select_peer);

            // send a request to that peer/seed
            MPI_Send(&rank, 1, MPI_INT, peer_id, REQ_TAG, MPI_COMM_WORLD);
            sendfilename(peer_id, file_name.data());
            sendhash(peer_id, downloads[file_name][i].second.data());
            // get reply
            recv_ack(peer_id);
            downloads[file_name][i].first = true;
        }

        update_client(file_name, file_size / 10 * 10, file_size % 10);

        int msg_type = FINISH_ONE;
        MPI_Send(&msg_type, 1, MPI_INT, TRACKER_RANK, OP_TAG, MPI_COMM_WORLD);
        sendfilename(TRACKER_RANK, file_name.data());

        // write hash to file
        char output_name[MAX_FILENAME];
        sprintf(output_name, "client%d_%s", rank, file_name.data());

        // open file to print output
        FILE *out = fopen(output_name, "ab+");
        for (auto map_pair : downloads[file_name]) {
            string hash = map_pair.second;
            fprintf(out, "%s\n", hash.data());
        }
        fclose(out);
    }

    int msg_type = FINISH_ALL;
    MPI_Send(&msg_type, 1, MPI_INT, TRACKER_RANK, OP_TAG, MPI_COMM_WORLD);
    return NULL;
}

void *upload_thread_func(void *arg)
{
    while (1) {
        int client_id;
        MPI_Recv(&client_id, 1, MPI_INT, MPI_ANY_SOURCE, REQ_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        if (client_id == TRACKER_RANK) {
            break;
        }

        string file_name = recvfilename(client_id);
        string hash = recvhash(client_id);

        send_ack_ok(client_id);
    }
    return NULL;
}

unordered_map<string, vector<vector<int>>> segm_owners;
unordered_map<string, vector<int>> seeds;

void tracker(int numtasks, int rank) {

    // for each client receive all files stored by them
    for (int i = 1; i < numtasks; i++) {
        int files_owned = recv1i(i);
        for (int j = 0; j < files_owned; j++) {
        
            string file_name = recvfilename(i);
            int file_size = recv1i(i);

            if (segm_owners.find(file_name) == segm_owners.end()) {
                vector<vector<int>> tmp(file_size, vector<int>({i}));
                segm_owners[file_name] = tmp;
            } else {
                for (int k = 0; k < file_size; k++)
                    segm_owners[file_name][i].push_back(i);
            }

            vector<string> file_data;
            for (int k = 0; k < file_size; k++) {
                string hash = recvhash(i);
                file_data.push_back(hash);
            }

            files[file_name] = file_data;
        }
    }

    //
    for (int i = 1; i < numtasks; i++) {
        send_ack_ok(i);
    }

    string file_name, hash;
    int file_size, hash_count, idx_start;
    int clients_finished = 0;
    bool running = true;
    while (running) {

        MPI_Status status;
        int msg_type = NO_MESSAGE;
        MPI_Recv(&msg_type, 1, MPI_INT, MPI_ANY_SOURCE, OP_TAG, MPI_COMM_WORLD, &status);
        
        int client_id = status.MPI_SOURCE;
        switch (msg_type) {
            case FILE_REQUEST:
                file_name = recvfilename(client_id);
                file_size = (int)files[file_name].size();

                // send data about the file requested
                send1i(client_id, &file_size);
                for (size_t i = 0; i < files[file_name].size(); i++) {
                    hash = files[file_name][i];
                    sendhash(client_id, hash.data());
                    sendlisti(client_id, segm_owners[file_name][i].data(), segm_owners[file_name][i].size());
                }
            break;
            case UPDATE_CLIENT:
                file_name = recvfilename(client_id);
                hash_count = recv1i(client_id);
                idx_start = recv1i(client_id);
                // update segments for the file with a new client
                for (int i = idx_start; i < idx_start + hash_count; i++) {
                    segm_owners[file_name][i].push_back(client_id);
                }
            break;
            case FINISH_ONE:
                // mark client as seed for finished file
                file_name = recvfilename(client_id);
                seeds[file_name].push_back(client_id);
            break;
            case FINISH_ALL:

                clients_finished++;
                if (clients_finished == numtasks - 1) {

                    // close all uploads
                    for (int i = 1; i < numtasks; i++) {
                        MPI_Send(&rank, 1, MPI_INT, i, REQ_TAG, MPI_COMM_WORLD);
                    }
                    running = false;
                }
            break;
        }
    }
}

void peer(int numtasks, int rank) {
    pthread_t download_thread;
    pthread_t upload_thread;
    void *status;
    int r;

    // read data from file
    int files_owned;
    char file_path[MAX_CHUNKS];
    sprintf(file_path, "in%d.txt", rank);
    FILE *fd = fopen(file_path, "r");
    fscanf(fd, "%d\n", &files_owned);
    for (int i = 0; i < files_owned; i++) {
        char file_name[MAX_FILENAME];
        int file_size = 0;
        fscanf(fd, "%s %d\n", file_name, &file_size);

        vector<string> file_data;
        for (int j = 0; j < file_size; j++) {
            char hash[HASH_SIZE];
            fscanf(fd, "%s\n", hash);
            file_data.push_back(hash);
        }
        files[file_name] = file_data;
    }
    int n;
    fscanf(fd, "%d\n", &n);
    for (int i = 0; i < n; i++) {
        char file_name[MAX_FILENAME];
        fscanf(fd, "%s\n", file_name);
        needed_files.push_back(file_name);
    }
    fclose(fd);

    // send data about the files owned to tracker
    send1i(TRACKER_RANK, &files_owned);
    for (auto file : files) {
        string file_id = file.first;
        int file_size = file.second.size();
        sendfilename(TRACKER_RANK, file_id.data());
        send1i(TRACKER_RANK, &file_size);
        for (int i = 0; i < file_size; i++) {
            sendhash(TRACKER_RANK, file.second[i].data());
        }
    }

    // receive confirmation from tracker
    recv_ack(TRACKER_RANK);
    
    r = pthread_create(&download_thread, NULL, download_thread_func, (void *) &rank);
    if (r) {
        printf("Eroare la crearea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_create(&upload_thread, NULL, upload_thread_func, (void *) &rank);
    if (r) {
        printf("Eroare la crearea thread-ului de upload\n");
        exit(-1);
    }

    r = pthread_join(download_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_join(upload_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de upload\n");
        exit(-1);
    }
}
 
int main (int argc, char *argv[]) {
    int numtasks, rank;
 
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided < MPI_THREAD_MULTIPLE) {
        fprintf(stderr, "MPI nu are suport pentru multi-threading\n");
        exit(-1);
    }
    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == TRACKER_RANK) {
        tracker(numtasks, rank);
    } else {
        peer(numtasks, rank);
    }

    MPI_Finalize();
}
