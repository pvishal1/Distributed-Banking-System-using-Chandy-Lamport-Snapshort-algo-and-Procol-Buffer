#! /usr/bin/env python

import socket
import sys
import time
import _thread
import threading
from threading import Thread
import random
from multiprocessing.pool import ThreadPool

import sys

sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')

import bank_pb2


class Snapshot(object):
    """
    Object to store snapshot information
    """
    def __init__(self, snapshot_id, balance):
        self.snapshot_id = snapshot_id
        self.balance = balance
        self.channels = {}
        self.recording_channels = []

    def __str__(self):
        return "%d, balance = %d\n channels:%s" % (self.snapshot_id,
                                                   self.balance,
                                                   str(self. channels))

# store socket of controller to send the retrieve msg

class branch():
    markerBalance = {}
    branchBalance = 0
    branchID = ""
    markerIdList = []
    branchList = []
    sockList = {}
    branchLock = threading.Lock()
    socketLock = threading.Lock()
    balanceLock = threading.Lock()
    markerLock = threading.Lock()
    markerListLock = threading.Lock()
    markerBalanceLock = threading.Lock()
    sendingMarker = False
    stateRecorder = {}
    s = ''
    controllerSocket = ''
    channelSnapshot = {}

    def __init__(self):
        self.name = sys.argv[1]
        self.ip = socket.gethostbyname(socket.gethostname())
        self.port = sys.argv[2]
        self.max_interval = sys.argv[3]

    def set_balance(self, value):
        self.branchBalance = value

    def get_balance(self):
        self.balanceLock.acquire()
        try:
            return self.branchBalance
        finally:
            self.balanceLock.release()

    def get_markerBalance(self):
        return self.markerBalance

    def set_markerBalance(self, id, value):
        self.markerBalanceLock.acquire()
        try:
            self.markerBalance[id] = value
        finally:
            self.markerBalanceLock.release()

    def add_balance(self, value):
        self.branchBalance += value

    def remove_balance(self, value):
        self.branchBalance -= value

    def start_branch(self):
        # print("START_BRANCH")
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.s.bind((self.ip, int(self.port)))
        print(self.ip, self.port)
        self.s.listen(100)
        while True:
            client_socket, client_add = self.s.accept()

            _thread.start_new_thread(self.branch_controller, (client_socket, client_add,))
            time.sleep(5)

    # Function to control the branches and the messages received by it
    def branch_controller(self, client_socket, client_add):
        while True:
            # if not self.sendingMarker:
            client_message = client_socket.recv(1024)
            # print(len(client_message), client_message)

            bm = bank_pb2.BranchMessage()
            bm.ParseFromString(client_message)
            msgType = bm.WhichOneof("branch_message")
            if msgType == "init_branch":
                # print("message received")
                self.controllerSocket = client_socket
                self.branchBalance = self.branchBalance + bm.init_branch.balance

                for branch in bm.init_branch.all_branches:
                    if self.name != branch.name:
                        self.branchList.append(branch)
                        time.sleep(1)
                        self.sockList[branch.name] = self.createSocketWOListen(branch)      #this creates the socket for each branch and store in dictionary

                # print("Sockets: ", self.sockList)
                time.sleep(5)
                _thread.start_new_thread(self.transferMoney, ())

            elif msgType == "transfer":
                # print("Receiving transfer message")
                rcvMoney = bm.transfer.money
                src_branch = bm.transfer.src_branch
                # rcvThread = Thread(target=self.receiveMoney(rcvMoney))
                # rcvThread.start()
                self.receiveMoney(rcvMoney, src_branch)

            elif msgType == "init_snapshot":
                # print("bm.init_snapshot.snapshot_id: ", bm.init_snapshot.snapshot_id)
                # print("received init_snapshot: ", self.markerIdList, bm, bm.init_snapshot.snapshot_id)
                # self.markerIdList.append(bm.init_snapshot.snapshot_id)
                _thread.start_new_thread(self.sendMarker, (bm.init_snapshot.snapshot_id,))

            elif msgType == "marker":
                sMarkerFlag = False
                if bm.marker.snapshot_id not in self.markerIdList:
                    sMarkerFlag = _thread.start_new_thread(self.sendMarker, (bm.marker.snapshot_id,))
                if not sMarkerFlag:
                    self.set_markerBalance(bm.marker.snapshot_id, self.get_balance())

            elif msgType == "retrieve_snapshot":
                _thread.start_new_thread(self.retrieveSnapshot, (bm,))

    def retrieveSnapshot(self, bm):
        rFlag = False
        # while not rFlag:
        snapId = bm.retrieve_snapshot.snapshot_id

        while snapId not in self.markerBalance:
            time.sleep(5)
            # self.retrieveSnapshot(bm)

        returnSnapshot = bank_pb2.ReturnSnapshot()
        localSnap = returnSnapshot.local_snapshot
        localSnap.snapshot_id = snapId
        localSnap.balance = self.markerBalance[snapId]
        for b in self.channelSnapshot:
            # if b in self.branchList:
            localSnap.channel_state.append(self.channelSnapshot[b])

        branchReturnMsg = bank_pb2.BranchMessage()
        branchReturnMsg.return_snapshot.CopyFrom(returnSnapshot)
        encodedReturnMsg = branchReturnMsg.SerializeToString()
        # print("branchReturnMsg: ", branchReturnMsg)
        self.controllerSocket.sendall(encodedReturnMsg)

    # function to continuously keep transferring the money to other branches. This keep running on different thread
    def transferMoney(self):
        # i = 4
        # while i != 0:
        while True:
            # print("transferring money to other branches")
            if not self.sendingMarker:
                # print("transferring")
                connBranchName = self.name
                while connBranchName == self.name:
                    connBranch = random.choice(self.branchList)
                    connBranchName = connBranch.name

                # print(connBranch.name, connBranch.ip, connBranch.port)

                if self.branchBalance > 0:
                    transferAmount = (random.randint(int(0.01 * self.branchBalance), int(0.05 * self.branchBalance)))

                    transferMsg = bank_pb2.Transfer()
                    transferMsg.src_branch = self.name
                    transferMsg.dst_branch = connBranch.name
                    transferMsg.money = transferAmount

                    branchMsg = bank_pb2.BranchMessage()
                    branchMsg.transfer.CopyFrom(transferMsg)
                    encodedMsg = branchMsg.SerializeToString()

                    # print("encoded transfer message: ", encodedMsg)

                    #connecting to branches
                    # connSock = self.sockList[connBranch.name]

                    if not self.sendingMarker:
                        # print("transferring sendingMarker: ",transferAmount)
                        # connSock.sendall(encodedMsg)
                        self.getSocketSendMsg(connBranch.name, encodedMsg)
                        # connSock.close()

                        self.update_balance(transferAmount, "remove")
                    # print("Balance after sending ", transferAmount, " : ", self.branchBalance)
                time.sleep(random.randrange(0, int(self.max_interval)/1000))
            # i = i-1
        # print("out of while in transfer money")

    #function to receive and update the money when transfer message is received
    def receiveMoney(self, rcvMoney, src_branch):
        # print("receiving money from ", src_branch, " branches: ", rcvMoney)
        self.update_balance(rcvMoney, "add", src_branch)
        # print("Balance after receiving ", rcvMoney,": ", self.branchBalance)

    def createSocketWOListen(self, branch):
        s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s1.connect((branch.ip, int(branch.port)))
        # s1.listen(5)
        return s1

    def getSocketSendMsg(self, branchName, msg):
        self.socketLock.acquire()
        try:
            connSock = self.sockList[branchName]
            connSock.sendall(msg)
            time.sleep(5)
        finally:
            self.socketLock.release()

    def update_balance(self, money, action, src = None):
        # print("acquiring lock")
        self.branchLock.acquire()
        try:
            if action == "add":
                self.channelSnapshot[src] = money
                self.add_balance(money)
                self.channelSnapshot[src] = 0
            else:
                self.remove_balance(money)
        finally:
            self.branchLock.release()
        # print("releasing lock")

    def update_markerList(self, markerID):
        # print("acquiring lock")
        self.markerListLock.acquire()
        try:
            if markerID not in self.markerIdList:
                self.markerIdList.append(markerID)
                return True
            return False
        finally:
            self.markerListLock.release()

    def sendMarker(self, snapshotId):
        # print("acquire markerLock")
        sMarkerF = False
        self.sendingMarker = True
        self.markerLock.acquire()
        try:
            if self.update_markerList(snapshotId):
                # print("Balance: ", self.get_balance())
                # snapshotId = branchMessage.init_snapshot.snapshot_id
                for branch in self.branchList:
                    markerMsg = bank_pb2.Marker()
                    markerMsg.src_branch = self.name
                    markerMsg.dst_branch = branch.name
                    markerMsg.snapshot_id = snapshotId

                    branchMsg = bank_pb2.BranchMessage()
                    branchMsg.marker.CopyFrom(markerMsg)
                    encodedMsg = branchMsg.SerializeToString()

                    # self.sockList[branch.name].sendall(encodedMsg)
                    self.getSocketSendMsg(branch.name, encodedMsg)
                    # print("Sending Marker Message to: ", branch.name, branchMsg)
                    sMarkerF = True
                return True
            return False

        finally:
            self.markerLock.release()
            self.sendingMarker = False
            return sMarkerF
            # print("release markerLock")


if __name__ == '__main__':
    con = branch()
    # con.start_branch()
    mainThread = Thread(target=con.start_branch())
    mainThread.start()
    # sys.exit(0)
