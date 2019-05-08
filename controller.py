import socket
import sys
sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import os
import mimetypes
import locale
import datetime
import threading
import time
import random
import bank_pb2


class controller:
    filename = ''
    branchSocket = {}
    branchList = []
    balance = 0
    snapshotId = 1

    def main(self):

        if len(sys.argv) != 3:
            print("Enter 3 arguments")
            sys.exit(0)

        self.filename = sys.argv[2]
        self.balance = sys.argv[1]



        if not os.path.isfile(self.filename):
            print("File not present in this location")
            sys.exit()


        nBranch = 0;
        with open(self.filename, 'rU') as fp:
            for i, l in enumerate(fp):
                pass
            nBranch = i + 1

        with open(self.filename, 'rU') as fp:

            branchBalance = int(self.balance)/nBranch


            for line in fp:

                conSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                branchName, branchIp, branchPort = line.split(' ')
                self.branchSocket[branchName] = conSocket
                # print(branchName, branchIp, branchPort)

                initBranchMsg = bank_pb2.InitBranch()
                initBranchMsg.balance = int(branchBalance)

                with open(self.filename, 'r') as fp1:
                    for line1 in fp1:
                        branchName1, branchIp1, branchPort1 = line1.split(' ')
                        # print(branchName1, branchIp1, branchPort1)

                        branchInfo = initBranchMsg.all_branches.add()
                        branchInfo.name = branchName1
                        branchInfo.ip = branchIp1
                        branchInfo.port = int(branchPort1)

                        if branchInfo not in self.branchList:
                            self.branchList.append(branchInfo)

                branchMsg = bank_pb2.BranchMessage()
                branchMsg.init_branch.CopyFrom(initBranchMsg)
                # print(branchMsg)
                encodedMsg = branchMsg.SerializeToString()

                # print(branchIp, branchPort)
                conSocket.connect((branchIp, int(branchPort)))
                # s.send(branchName)
                conSocket.sendall(encodedMsg)

                # while True:
                #     time.sleep(5)
                #     self.InitSnapshot()
                # s.close()
        while True:
            time.sleep(3)
            self.InitSnapshot()
            time.sleep(5)
            self.RetrieveSnapshot()
            time.sleep(3)



    def createSocketSendMsg(self, branch, msg):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((branch.ip, int(branch.port)))
        s.sendall(msg)
        s.close()

    def InitSnapshot(self):
        # print(self.branchList)
        selBranch = random.choice(self.branchList)
        snapshotIdLock = threading.Lock()

        initSnapshotMsg = bank_pb2.InitSnapshot()
        initSnapshotMsg.snapshot_id = self.snapshotId
        branchMsg = bank_pb2.BranchMessage()
        branchMsg.init_snapshot.CopyFrom(initSnapshotMsg)
        # print(branchMsg)
        initSnapEncodedMsg = branchMsg.SerializeToString()

        # initSnapSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # initSnapSocket.connect((selBranch.ip, int(selBranch.port)))

        self.branchSocket[selBranch.name].sendall(initSnapEncodedMsg)
        # initSnapSocket.close()

        snapshotIdLock.acquire()
        try:
            self.snapshotId += 1
        finally:
            snapshotIdLock.release()

    def RetrieveSnapshot(self):
        for branch in self.branchList:
            snapshotIdLock = threading.Lock()

            retSnapshotMsg = bank_pb2.RetrieveSnapshot()
            retSnapshotMsg.snapshot_id = self.snapshotId - 1
            branchMsg = bank_pb2.BranchMessage()
            branchMsg.retrieve_snapshot.CopyFrom(retSnapshotMsg)
            retSnapEncodedMsg = branchMsg.SerializeToString()
            # print(branchMsg)
            self.branchSocket[branch.name].sendall(retSnapEncodedMsg)

        time.sleep(3)

        print("\nsnapshot_id: ", str(self.snapshotId - 1))
        for branch in self.branchList:
            # print(self.branchSocket[branch.name])
            returnSnap = self.branchSocket[branch.name].recv(1024)
        #     bsoc, baddr = self.branchSocket[branch.name].accept()
        #     returnSnap = bsoc.recv(1024)
            bm = bank_pb2.BranchMessage()
            bm.ParseFromString(returnSnap)
            branchString = branch.name + ": " + str(bm.return_snapshot.local_snapshot.balance) + ", "
            # print("return snapshot msg: ", bm)
            cList = bm.return_snapshot.local_snapshot.channel_state
            # print(cList)
            i = 0
            for sisterBranch in self.branchList:
                if sisterBranch.name != branch.name:
                    if i >= len(cList) or cList[i]:
                        branchString += str(sisterBranch.name) + "->" + str(branch.name) + ": 0\t"
                    else:
                        branchString += str(sisterBranch.name) + "->" + str(branch.name) + ": " + str(cList[i]) + "\t"
                    i = i+1
            print(branchString)




if __name__ == '__main__':
    con = controller()
    con.main()
    sys.exit(0)