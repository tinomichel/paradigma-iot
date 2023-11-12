import socket

systalan_ip = "192.168.188.34"
systalan_port = 7260
systalan_pw = "31323334"
MaxDataLength  = 2048
Code = "0a0114e1"


sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
#data    4950515210120225 
#command 313233340a0114e1
#1234
monitorcommand = "313233340a0114e1"
print(monitorcommand)
print monitorcommand.decode("hex")
sock.sendto(monitorcommand.decode("hex"), (systalan_ip, systalan_port))
msgFromServer = sock.recvfrom(MaxDataLength)
print(msgFromServer)
