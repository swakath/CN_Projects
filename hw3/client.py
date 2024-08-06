import select
import socket
import hashlib
import time
import matplotlib.pyplot as plt

FORMAT = "utf-8"
SUBMIT_MSG = "Submit: "
TEAM_ID_MSG = "2023MCS2483@encoder\n"
SENDSIZE_MSG = "SendSize\n\n"
MD5_MSG = "MD5: "
RESET_SERVER = "SendSize\nReset\n\n"
expectSize = 0
dataReceivedSize = 0
receive_start_time = 0
request_data = []
reply_data = []
offsets = []
Offset_hashmap = {}


def totalDataReceived():
    global dataReceivedSize
    global expectSize

    if dataReceivedSize == expectSize:
        return True
    return False


def getData(socket_id, currentOffset):
    global Offset_hashmap, dataReceivedSize

    readable, _, _ = select.select([socket_id], [], [], 0.3)
    if readable:
        data = socket_id.recv(4096).decode(FORMAT)
        [serverReplyInfo, recvData] = data.split("\n\n")
        [offset, numBytes] = serverReplyInfo.split("\n")

        offset_val = int(offset.split(":")[1][1:])
        numBytes_val = int(numBytes.split(":")[1][1:])
        Offset_hashmap[offset_val] = recvData
        # print(recvData)
        # print(data)
        dataReceivedSize += numBytes_val

        return currentOffset + numBytes_val
    else:
        return currentOffset


def submitToVayu(client_socket, md5_hash):
    # Submitting the data by calculating the MD5 hash
    submission = SUBMIT_MSG + TEAM_ID_MSG + MD5_MSG + str(md5_hash) + "\n\n"
    client_socket.sendall(submission.encode(FORMAT))

    readable, _, _ = select.select([client_socket], [], [], 0.5)
    if readable:
        submit_reply = client_socket.recv(1024).decode(FORMAT)
        [result, time, penalty, _, _] = submit_reply.split("\n")
        if result.split(":")[1][1:] == "true":
            print(submit_reply)
            return True
        else:
            print(submit_reply)
            return False


def getMD5Hash():
    global Offset_hashmap
    dataRecvd = ""
    # sorted_OffsetHashmap = dict(sorted(Offset_hashmap.items(), key=lambda x: x[0]))
    sorted_OffsetHashmap = Offset_hashmap

    for key, value in sorted_OffsetHashmap.items():
        dataRecvd += value
    md5hash = hashlib.md5()
    md5hash.update(dataRecvd.encode())
    return md5hash.hexdigest()


def getDataFromVayu(server_dn, server_port, reset):
    global expectSize, receive_start_time

    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    except socket.error as err:
        print("{NOK} Server Socket creation error [%s]" % (err))

    print("[OK] Server Socket successfully created")
    serrver_addr = (server_dn, server_port)

    try:
        client_socket.connect(serrver_addr)
        print("[OK]: Connect to DN [%s] Port [%s]" % (server_dn, server_port))
        if reset:
            client_socket.sendall(RESET_SERVER.encode(FORMAT))
        else:
            client_socket.sendall(SENDSIZE_MSG.encode(FORMAT))

        receive_start_time = time.time()
        recvData = client_socket.recv(1024).decode(FORMAT)
        print("Server reply : [%s]" % (recvData.strip("\n")))

        expectSize = int(recvData.split(":")[1])
        offset = 0
        try:
            while not totalDataReceived():
                temp = min(1448, expectSize - offset)
                message = f"Offset: {offset}\nNumBytes: {temp}\n\n"
                client_socket.sendall(message.encode(FORMAT))
                offsets.append(offset)
                req_time = time.time() - receive_start_time
                request_data.append(req_time)
                res_val = getData(client_socket, offset)
                if offset == res_val:
                    reply_data.append(None)
                    continue
                elif res_val <= expectSize:
                    offset = res_val
                    rep_time = time.time() - receive_start_time
                    reply_data.append(rep_time)
                else:
                    break
                time.sleep(0.005)
            print("Data received successfully")
            md5hash = getMD5Hash()
            result = submitToVayu(client_socket, md5hash)
            if result:
                print("Submission successful")
            else:
                print("Submission unsuccessful")
        finally:
            client_socket.close()

    except ConnectionRefusedError:
        print("[NOK] DEBUG tID[%s]:- Socket connect error")


def printGraph():
    global request_data, reply_data
    plt.scatter(request_data, offsets, color="r", label="Request", s=60)
    plt.scatter(reply_data, offsets, color="g", label="Reply", s=30)
    plt.xlabel("Time in seconds")
    plt.ylabel("Offsets")
    plt.title("Reliable Data Transfer using UDP")
    plt.legend()
    plt.savefig("UDP_RDT.png", format="png", bbox_inches="tight")


def main():
    print("Starting...")

    vayu_dn = "127.0.0.1"
    PORT = 9801

    getDataFromVayu(vayu_dn, PORT, True)
    printGraph()


main()
