# TCP Slave for retriving file from vayu.iitd.ac.in and send it to master
# Author        : S U Swakath, Aakash
# Email         : suswakath@gmail.com

# Importing libraries
import select
import socket
import threading
import time
import pickle

DISCONNECT_MSG = "DISCONNECT!"
FORMAT = "utf-8"
SUBMIT_MSG = "SUBMIT\n"
TEAM_ID_MSG = "2023MCS2475@encoder\n"

SLAVE_OK = "SLAVE_OK\n\n"
SLAVE_START = "SLAVE_START\n\n"
SLAVE_STOP = "SLAVE_STOP\n\n"
SLAVE_END = "SLAVE_END\n\n"
SLAVE_DEBUG = "SLAVE_DEBUG\n\n"

Line_Hashmap = {}
received_lines=[0]*1000

def setHashMap(binMap):
    global Line_Hashmap
    Line_Hashmap = pickle.loads(binMap)

def submitTOvayu(client_socket):
    # Submitting received lines on vayu
    global Line_Hashmap
    global SUBMIT_MSG
    global TEAM_ID_MSG
    global TOTAL_LINES_READ_MSG
    global FORMAT

    lines_read = len(Line_Hashmap)
    TOTAL_LINES_READ_MSG = str(lines_read) + "\n"
    client_socket.sendall(SUBMIT_MSG.encode(FORMAT))
    client_socket.sendall(TEAM_ID_MSG.encode(FORMAT))
    client_socket.sendall(TOTAL_LINES_READ_MSG.encode(FORMAT))
    client_socket.setblocking(False)

    for key, value in Line_Hashmap.items():
        LINE_MSG = f"{key}\n{value}\n"
        client_socket.sendall(LINE_MSG.encode(FORMAT))
        readable, _, _ = select.select([client_socket], [], [], 0.03)
        if readable:
            submit_reply = client_socket.recv(1024).decode(FORMAT)
            if submit_reply[:13] == "SUBMIT FAILED":
                print(submit_reply)
                break
            else:
                times = submit_reply.split("-")[3].split(",")
                start_time, finish_time = int(times[0]), int(times[2])
                print(submit_reply)
                print(f"Time taken {finish_time - start_time}")
                break
    return True

def getData(socket_id):
    line_cnt = 0
    deData = ""
    while line_cnt != 2:
        data = socket_id.recv(1024)
        deData = deData + data.decode(FORMAT)
        line_cnt = deData.count("\n")
    return deData

def receivedLinesSum():
    global received_lines
    return sum(received_lines)

def updateLineInfo(line_no):
    global received_lines
    if received_lines[line_no] != 1:
        received_lines[line_no] = 1
    return True


def getLineFromVayuSlave(server_dn, server_port,master_socket):
    global SLAVE_OK
    global SLAVE_START
    global SLAVE_STOP
    global SLAVE_END
    global SLAVE_DEBUG
    global FORMAT

    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except socket.error as err:
        print("[NOK] Server Socket creation error [%s]" % (err))

    print("[OK] Server Socket successfully created")
    server_addr = (server_dn, server_port)

    try:
        client_socket.connect(server_addr)
        print(
            "[OK]: Connect to DN [%s] Port [%s]"
            % (server_dn, server_port)
        )
        # reset_server = "SESSION RESET\n"
        # client_socket.sendall(reset_server.encode(FORMAT))
        # recvData = client_socket.recv(1024).decode(FORMAT)
        # print("Server Reset Reply: [%s]" %(recvData))

        try:
            master_socket.sendall(SLAVE_START.encode(FORMAT))
        
            deData_master = getData(master_socket)
            if(deData_master == SLAVE_OK):
                message = "SENDLINE\n"
                try:
                    while (receivedLinesSum() != 1000):
                        client_socket.sendall(message.encode(FORMAT))
                        line_cnt = 0
                        deData = ""
                        deData = getData(client_socket)

                        print(deData[0:2])
                        if deData[0:2] != "-1":
                            lines = deData.split("\n")
                            line_no = int(lines[0])
                            updateLineInfo(line_no)
                            master_socket.sendall(deData.encode())
                            line_cnt = 0
                            deData_master = ""
                            deData_master = getData(master_socket)
                            if deData_master == SLAVE_OK:
                                continue
                            elif deData_master == SLAVE_STOP:
                                binObj = b''
                                while(binObj.endswith(bytes(SLAVE_END, "utf-8")) == False):
                                    binObj = binObj + master_socket.recv(1024)
                                binObj = binObj[:-9]
                                setHashMap(binObj)
                                print("Submitting to vayu: ")
                                submitTOvayu(client_socket)
                                break
                            else:
                                break
                    print("[OK] Slave Connection End")
                except:
                    print("[NOK] Connection Error 2")
        except:
                print("[NOK] Connection Error 1")
        client_socket.close()
    except ConnectionRefusedError:
        print("[NOK] DEBUG tID[%s]:- Socket connect error")
    master_socket.close()

def main():
    print("Hello")
    #vayu_dn = "vayu.iitd.ac.in"
    vayu_dn = "10.17.6.5"
    PORT = 9801

    master_ip="10.194.62.0"
    MASTER_PORT = 8080

    try:
        mater_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except socket.error as err:
        print("[NOK] Master Socket creation error [%s]" % (err))
    print("[OK] Master Socket successfully created")

    master_addr = (master_ip, MASTER_PORT)
    
    try:
        mater_socket.connect(master_addr)
    except:
        print("[NOK] Unable to connect to master")
    getLineFromVayuSlave(vayu_dn,PORT,mater_socket)
    
main()
