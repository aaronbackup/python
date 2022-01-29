import struct
import socket
import time
import datetime
host_port_6666=("10.36.120.83",6666)
host_port_8888=("10.36.120.83",8888)

loss_file="../bin/api_flow/XtpApi_md/log/udpseq2_2." + datetime.date.today().strftime("%Y%m%d")
if __name__ == "__main__":
    print (loss_file)
    count = len(open(loss_file,'rU').readlines())
    print("lines count " + str(count.))
    if(count > 1) :
        sk = socket.socket()
        sk.connect(host_port_8888)
        buff = struct.pack('iii',8,142,count)
        sk.send(buff)
        time.sleep(2)
        sk.close()


