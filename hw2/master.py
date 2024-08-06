# TCP Master for retriving file from vayu.iitd.ac.in and slaves and send the final
# assembled file to all slaves and server.
# Author        : S U Swakath, Aakash
# Email         : suswakath@gmail.com

# Importing libraries
import select
import socket
import threading
import time
import pickle
import matplotlib.pyplot as plt

DISCONNECT_MSG = "DISCONNECT!"
FORMAT = "utf-8"
SUBMIT_MSG = "SUBMIT\n"
TEAM_ID_MSG = "2023MCS2483@encoder\n"

SLAVE_OK = "SLAVE_OK\n\n"
SLAVE_START = "SLAVE_START\n\n"
SLAVE_STOP = "SLAVE_STOP\n\n"
SLAVE_END = "SLAVE_END\n\n"
SLAVE_DEBUG = "SLAVE_DEBUG\n\n"
Line_Hashmap = {}
analysis = []
received_lines=[0]*1000
receive_start_time = 0

def getPickle():
    global Line_Hashmap
    return pickle.dumps(Line_Hashmap)

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

def updateLineInfo(line_no, line_content):
    global received_lines
    global Line_Hashmap
    global analysis
    global receive_start_time
    if received_lines[line_no] != 1:
        received_lines[line_no] = 1
        time_now = time.time()
        elapsed = (time_now - receive_start_time) * 1000
        analysis.append(elapsed)
        Line_Hashmap[line_no] = line_content
    return True

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

def printGraph(data):
    plt.plot(data)
    plt.xlabel("Number of lines")
    plt.ylabel("Processing Steps")
    plt.title("Effeciency plot")
    plt.savefig("line_plot.jpg", format="jpg")


# Server socket function
def getLineFromVayu(thread_run, server_dn, server_port,lock):
    global receive_start_time
    tID = threading.get_ident()

    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except socket.error as err:
        print("[NOK] DEBUG tID[%s]:- Socket creation error [%s]" % (tID, err))

    print("[OK] DEBUG tID[%s]:- Socket successfully created" % (tID))
    server_addr = (server_dn, server_port)

    try:
        client_socket.connect(server_addr)
        print(
            "[OK] DEBUG tID[%s]:- Connect to DN [%s] Port [%s]"
            % (tID, server_dn, server_port)
        )
        # reset_server = "SESSION RESET\n"
        # client_socket.sendall(reset_server.encode(FORMAT))
        # recvData = client_socket.recv(1024).decode(FORMAT)
        # print("Server Reset Reply: [%s]" %(recvData))
        receive_start_time = time.time()
        message = "SENDLINE\n"
        interval = 1.0 / 100
        try:
            # Reading lines from vayu server
            while True:
                #lock.acquire()
                if(receivedLinesSum() != 1000):
                    client_socket.sendall(message.encode(FORMAT))
                    deData = getData(client_socket)
                    #print(deData[0:2])
                    if deData[0:2] != "-1":
                        lines = deData.split("\n")
                        line_no = int(lines[0])
                        lock.acquire()
                        updateLineInfo(line_no, lines[1])
                        lock.release()
                else:
                    #lock.release()
                    break
            print(
                "[OK] DEBUG tID[%s]:- Submitting to Vayu"
                % (tID)
            )

            submitTOvayu(client_socket)

            print(
                "[OK] DEBUG tID[%s]:- Received:[%s][%s][%s]"
                % (tID, len(analysis), analysis[999], analysis[0])
            )

        finally:
            client_socket.close()

    except ConnectionRefusedError:
        print("[NOK] DEBUG tID[%s]:- Socket connect error" % (tID))

    print("[OK] DEBUG tID[%s]:- Thread End" % (tID))

def getLineFromSlave(thread_run_slave, slave_socket, slave_addr,lock):
    global SLAVE_OK
    global SLAVE_START
    global SLAVE_STOP
    global SLAVE_END
    global SLAVE_DEBUG

    tID = threading.get_ident()
    print("[OK] DEBUG tID[%s]:- Slave Connected [%s]" % (tID, slave_addr))

    deData = getData(slave_socket)
    if(deData == SLAVE_START):
        slave_socket.sendall(SLAVE_OK.encode(FORMAT))
        print("[OK] DEBUG tID[%s]:- Slave Comm Initiated [%s]" % (tID, slave_addr))
        while True:
            #print("[OK] DEBUG tID[%s]:- In while" % (tID))
            deData = ""
            deData = getData(slave_socket)
            #print("[OK] DEBUG tID[%s]:- [%s]" % (tID,deData))   
            
            if(deData == SLAVE_END):
                print("[OK] DEBUG tID[%s]:- Comm End From Slave [%s]" % (tID, slave_addr))
                break
            if(deData == SLAVE_DEBUG):
                print("[OK] DEBUG tID[%s]:- Slave Comm debug [%s]" % (tID, slave_addr))
                slave_socket.sendall(SLAVE_OK.encode(FORMAT))
                continue

            lines = deData.split("\n")
            line_no = int(lines[0])    
            #lock.acquire()
            if(receivedLinesSum() != 1000):
                lock.acquire()
                updateLineInfo(line_no, lines[1])
                lock.release()
                #print("[OK] DEBUG tID[%s]:- Received line from slave debug [%s]" % (tID, slave_addr))
                slave_socket.sendall(SLAVE_OK.encode(FORMAT))
            else:
                #lock.release()
                slave_socket.sendall(SLAVE_STOP.encode(FORMAT))
                binObj = getPickle()
                binObj = binObj + bytes(SLAVE_END,"utf-8")
                slave_socket.sendall(binObj) 
                print("[OK] DEBUG tID[%s]:- Comm End From Master [%s]" % (tID, slave_addr))
                break

    slave_socket.close()
        
def main():
    threads=[]
    lock = threading.Lock()
    print("Hello")
    #vayu_dn = "vayu.iitd.ac.in"
    vayu_dn = "10.17.51.115"
    #vayu_dn = "10.17.6.5"
    #vayu_dn = "10.17.7.134"
    PORT = 9801

    local_server_ip="10.194.40.25"
    HOST_PORT = 8080

    thread_run_vayu = True
    thread_run_slave = True
    t1 = threading.Thread(
       target=getLineFromVayu, args=(lambda: thread_run_vayu, vayu_dn, PORT,lock)
    )
    threads.append(t1)
    t1.start()

    try:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except socket.error as err:
        print("[NOK]: Server Socket creation error [%s]" % (err))
    print("[OK]: Server Socket created successfully")
    
    try:
        server_socket.bind((local_server_ip, HOST_PORT))
    except:
        print("[NOK]: Server Socket Bind error")
    print("[OK]: Server Socket Bind Successful")

    try:
        server_socket.listen(5)
    except:
        print("[NOK]: Server Socket Listen error")
    print("[OK]: Server Socket Listen Successful")        
           
    for i in range(3):
        if(receivedLinesSum == 1000):
            break

        cur_slave_socket, cur_slave_addr = server_socket.accept()    
        new_thread = threading.Thread(
            target=getLineFromSlave, args=(lambda: thread_run_slave, cur_slave_socket, cur_slave_addr,lock)
        )
        new_thread.start()
        threads.append(new_thread)
    
    
    for t in threads:
        t.join()
    printGraph(analysis)

main()
