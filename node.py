#!/usr/bin/env python3

import json
import socket
import threading
import time
import random


FORMAT = 'utf-8'

# Liste des commande 
JOIN =    "JOIN"
ACCEPT =  "accept"
REJECT =  "reject"
ACK =     "ack"
GET =     "get"
PUT =     "put"
UPDATE =  "update_table"
ANSWER =  "answer"
STATS =   "stats"
PRINT =    "print"
GET_RESP = "get_rep"
ANSWER_RESP = "answer_resp"

class Node():

    # Node attributes 
    nodeIP_adress  = "127.0.0.1" 
    nodePort  = None  
    nodeID     = None 
    nodeData = None  # donnee du noeud resp
    nodePred =  None  # noeud pred 
    TV = []  # table de voisins des noeuds 

    # attributes for the protocole
    MAX_NODE = None  
    NB_V = 8 # nbre de voisin = Ln2(MAX_NODE)
    BUFFER_SIZE = None  # taille du buffer utilise dans les communication sockets 

    # variable globales pour les statistiques 
    NB_JOIN = 0 
    NB_GET = 0 
    NB_PUT = 0 


    # initialisation
    def __init__(self,notFirst,nodeID, nodeIP_adress ,nodePort, lambdaNode=None, max_node=65536, BUFFER_SIZE = 65565):
        
        # attributes initialisation
        self.nodeIP_adress = nodeIP_adress
        self.nodePort = nodePort
        self.MAX_NODE = max_node
        self.BUFFER_SIZE = BUFFER_SIZE
       
        if notFirst:
            id = nodeID
            CMD = { 
                "cmd": JOIN,
                "args":{
                    "host":{    
                        "IP": self.nodeIP_adress,
                        "port":self.nodePort,
                        "idNode": id
                    }
                }
            }
            self.Send_Command(lambdaNode,CMD)
            self.NB_JOIN +=1 # statistics

            # attendre la rep
            msg = self.Wait_Command()

            if msg["cmd"]==ACCEPT :

                self.nodeID = msg["args"]["id_requested"]
                self.nodePred = (msg["args"]["info_previous_node"]["IP"],msg["args"]["info_previous_node"]["port"],msg["args"]["info_previous_node"]["idNode"])

                self.TV.append( ( msg["args"]["id_requested"]+1,
                                ( msg["args"]["info_resp_node"]["IP"],msg["args"]["info_resp_node"]["port"],msg["args"]["info_resp_node"]["idNode"])))
                self.nodeData = (msg["args"]["data"]["borne1"],msg["args"]["data"]["borne2"],msg["args"]["data"]["keys"])

                #obtenir le responsable de la TV et les mettre à jour
                for i in range(1,self.NB_V):

                    j = self.nodeID + 2**i
                    #si succ est le resp 
                    if(self.is_between(j, self.nodeID ,msg["args"]["info_resp_node"]["idNode"])):
                        self.TV.append( ( j,
                                ( msg["args"]["info_resp_node"]["IP"],msg["args"]["info_resp_node"]["port"],msg["args"]["info_resp_node"]["idNode"])))
                    else:                    
                        send_CMD = {
                            "cmd":GET_RESP, 
                            "args":{
                                "host":{
                                    "IP": self.nodeIP_adress, 
                                    "port": self.nodePort, 
                                    "idNode":self.nodeID
                                }, 
                                "key": j 
                            }
                        }
                        self.Send_Command(lambdaNode,send_CMD)
                        self.NB_JOIN +=1 # statistics
                        msg1 = self.Wait_Command()
                        print("asssim t list",i)
                        self.TV.append( ( j,
                                ( msg1["args"]["resp"]["IP"],msg1["args"]["resp"]["port"],msg1["args"]["resp"]["idNode"])))
                
                # mettre a jour les TV          
                send_CMD = {
                    "cmd":UPDATE, 
                    "args":{
                        "src":{ # les infos du noeud insere
                            "IP": self.nodeIP_adress, 
                            "port": self.nodePort, 
                            "idNode":self.nodeID
                        }, 

                    }
                }
                self.Send_Command(self.nodePred[0:2],send_CMD)
                print("my data",self.TV)
                self.listen()
            else:
                print("node not inserted")  
        else:
            self.nodeID = nodeID
            self.nodePred = (self.nodeIP_adress,self.nodePort,self.nodeID)
            self.nodeData = (self.nodeID +1, self.nodeID,{})
            print(self.nodeData[0])
            for j in range(0,self.NB_V):
                self.TV.append((self.nodeID+ 2**j,(self.nodeIP_adress,self.nodePort,self.nodeID)))
            print("my data",self.TV)
            self.listen()     

    def listen(self):
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self.nodeIP_adress,self.nodePort))
            server.listen()
            print(f"[LISTENING] The server is listening on {(self.nodeIP_adress,self.nodePort)}.")

            conn, addr = server.accept()
            thread = threading.Thread(target=self.handle_Node, args=(conn,addr))
            thread.start()
            conn.close()
            server.close()

    def is_between(self, nodeID, borne1, borne2):
        if borne1 < borne2:
            if (nodeID >= borne1 and nodeID <= borne2) : 
                return True
            else :  
                return False
        else :
            if (nodeID >= borne1) or (nodeID <= borne2): 
                return True
            else :
                return False

        return False


    def handle_join(self,CMD):
        if CMD["args"]["host"]["idNode"] != self.nodeID:
            resp =[]
            for i in range(0,self.NB_V):
                resp.append(self.TV[i][1][2])
            resp.append(self.nodePred[2])
            print("resp = ",resp)
            if  CMD["args"]["host"]["idNode"] in resp :
                send_CMD = {  
                    "cmd" : REJECT, 
                    "args" : { 
                        "key": CMD["args"]["host"]["idNode"]
                    }
                }
                self.Send_Command((CMD["args"]["host"]["IP"],CMD["args"]["host"]["port"]),send_CMD)
                self.NB_JOIN +=1 # statistics
            else:
                if self.is_between(CMD["args"]["host"]["idNode"],self.nodePred[2] ,self.nodeID):
                    send_CMD = {  
                        "cmd" : ACCEPT, 
                        "args" : { 
                            "id_requested": CMD["args"]["host"]["idNode"], 
                            "info_resp_node" : {
                                "IP": self.nodeIP_adress,
                                "port":self.nodePort,
                                "idNode": self.nodeID
                            },
                            "data": {
                                "borne1": self.nodeData[0],
                                "borne2": CMD["args"]["host"]["idNode"],
                                "keys": dict( (key, value) for (key, value) in self.nodeData[2].items() if key <= CMD["args"]["host"]["idNode"] )
                            }, 
                            "info_previous_node": {
                                "IP": self.nodePred[0],
                                "port":self.nodePred[1],
                                "idNode": self.nodePred[2]
                            }
                        }
                    }
                    self.Send_Command((CMD["args"]["host"]["IP"],CMD["args"]["host"]["port"]),send_CMD)
                    self.NB_JOIN +=1 
                    # changer de prédécesseur et supprimer les nœuds dont il n'est plus responsable
                    self.nodePred = (CMD["args"]["host"]["IP"],CMD["args"]["host"]["port"],CMD["args"]["host"]["idNode"])
                    self.nodeData = (CMD["args"]["host"]["idNode"]+1 ,self.nodeData[1], dict( (key, value) for (key, value) in self.nodeData[2].items() if key > CMD["args"]["host"]["idNode"]))
                else:
                    #trouver le resp et lui transmettre la cmd
                    resp = self.Find_resp(CMD["args"]["host"]["idNode"])
                    self.Send_Command(resp[0:2],CMD)
                    self.NB_JOIN +=1         
        else:
            send_CMD = {  
                "cmd" : REJECT, 
                "args" : { 
                    "key": CMD["args"]["host"]["idNode"]
                }
            }
            self.Send_Command((CMD["args"]["host"]["IP"],CMD["args"]["host"]["port"]),send_CMD)
            self.NB_JOIN +=1 
    
    # fonct pour envoyer des cmd aux noeuds 
    def Send_Command(self,node,CMD):
        try:
            conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
            conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            conn.bind((self.nodeIP_adress,self.nodePort))      
            conn.connect(node)
            print(json.dumps(CMD))
            conn.send(json.dumps(CMD).encode(FORMAT))
            conn.close()
        except socket.error as exc:
            print("error while connecting to node"+ str(exc))

    def Wait_Command(self):
        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self.nodeIP_adress,self.nodePort))
            server.listen()
            conn, addr = server.accept()
            msg = conn.recv(self.BUFFER_SIZE)
            print(msg.decode(FORMAT))
            conn.close()
            server.close()
            return json.loads(msg)
        except socket.error as exc:
            print("error while connecting to node"+ str(exc))

    def PUT_CMD(self, Dest, NewValue):
        CMD = { 
            "cmd": PUT,
            "args":{
                "host":{ 
                    "IP": self.nodeIP_adress,
                    "port":self.nodePort,
                    "idNode":self.nodeID
                },
                "key": Dest,
                "value": NewValue,
                "id":"put-" + str(self.nodeID)+"-"+str(self.NB_PUT)
            }
        }
        self.handle_put(CMD)

    def handle_put(self, CMD):
        # vérifier si je suis le responsable
        if self.is_between(CMD["args"]["key"],self.nodePred[2]+1, self.nodeID):

            #Je suis le resp, trouver la valeur dans de mes données et la modifier
            self.nodeData[2][CMD["args"]["key"]] = CMD["args"]["value"]
            # Réponse à un put. Le nœud cible va répondre en direct au nœud d’origine avec l’idUniq pour lui accuser réception de son put.
            send_CMD = { 
                "cmd": ACK,
                "args" : {
                     "ok ":"ok",
                     "idUniq": CMD["args"]["id"]
                }
            }
            self.Send_Command((CMD["args"]["host"]["IP"],CMD["args"]["host"]["port"]),send_CMD)
            self.NB_PUT +=1
        else:
            resp = self.Find_resp(CMD["args"]["key"])
            self.Send_Command(resp[0:2],CMD)
            self.NB_PUT +=1             
    
    def GET_CMD(self,Dest):
        CMD = { 
            "cmd": GET,
            "args":{
                "host":{ 
                    "IP": self.nodeIP_adress,
                    "port":self.nodePort,
                    "idNode":self.nodeID
                },
                "key": Dest
            }
        }
        self.handle_get(CMD)

    #Demande des données du nœud avec la clé (key) et fournit son adresse (ip/port) pour la réponse.
    def handle_get(self, CMD):
        # vérifier si je suis le responsable
        if self.is_between(CMD["args"]["key"],self.nodePred[2]+1, self.nodeID):
            print("responsable du noeud")
            # je suis le resp du noeud, je rep a la requete
            #Réponse à un get. Le noeud responsable répond directement à celui qui avait demandé
            #en lui redonnant la clé (key) des valeurs demandées ainsi que les valeurs (val) ou None s’il n’y a pas de valeurs pour cette clé. 
            
            send_CMD = { 
                "cmd": ANSWER,
                "args" : {
                     "key" : CMD["args"]["key"],
                     "value" : self.nodeData[2][CMD["args"]["key"]] if  CMD["args"]["key"] in self.nodeData[2].keys() else None,
                     "Value_Exist" : CMD["args"]["key"] in self.nodeData[2].keys() 
                }
            }

            self.Send_Command((CMD["args"]["host"]["IP"],CMD["args"]["host"]["port"]),send_CMD)
            self.NB_PUT +=1 # statistics
        else:
            resp = self.Find_resp(CMD["args"]["key"])
            print("resp", resp)
            self.Send_Command(resp[0:2],CMD)
            self.NB_GET +=1 
        


    def get_stats(self):
        CMD = {
            "cmd" : "stats", 
            "args" : { 
                "source":{
                    "IP": self.nodeIP_adress,
                    "port": self.nodePort,
                    "idNode": self.nodeID 
                }, 
                "nb_get": self.NB_GET, 
                "nb_put": self.NB_PUT, 
                "NB_JOIN": self.NB_JOIN
            }
        }
        self.Send_Command(self.nodePred[0:2],CMD)


    def Find_resp(self,key_id):
        j=0 
        while j < self.NB_V and key_id != self.TV[j][0] and key_id >= self.TV[j][1][2]:
            j +=1
        if j < self.NB_V and key_id == self.TV[j][0]: 
            #si le noeud exite dans la table,on sait que c'est lui le resp
            return self.TV[j][1]
        else: 
            #on l'envoi au nœud le plus grand mais inférieur
            return self.TV[j-1][1]



#handle the commands coming to this node
    def handle_Node(self,conn,addr):
        msg = json.loads(conn.recv(self.BUFFER_SIZE).decode(FORMAT))
        print(" received: ------------------------")
        print(msg)
        print("-----------------------------------")

        if msg["cmd"] == JOIN:
            self.handle_join(msg)
        elif msg["cmd"] == PUT:
            self.handle_put(msg)
        elif msg["cmd"] == ACK:
            print("put msg with id "+ str(msg["args"]["idUniq"])+" is successufly received")
        elif msg["cmd"] == UPDATE:
            # on vérifie les voisins si leur responsable est changé
            print(msg)
            for j in range(0,self.NB_V):
                if self.TV[j][0] != self.TV[j][1][2] and self.is_between(msg["args"]["src"]["idNode"],self.TV[j][0],self.TV[j][1][2]):
                    self.TV[j] =(self.TV[j][0], (msg["args"]["src"]["IP"],msg["args"]["src"]["port"],msg["args"]["src"]["idNode"]))

            ## après que la TV a mis à jour les nœuds vers prec si mon pred n'est pas le nœud qui vient d'être inséré (mise à jour sur cercle)
            if self.nodePred[2] != msg["args"]["src"]["idNode"]:
                self.Send_Command(self.nodePred[0:2],msg)
            
        elif msg["cmd"]== GET_RESP:
            # if i am the responsable (or can find it in my table send) or forward it to the closest node
            if  msg["args"]["key"] != self.nodePred[2] and self.is_between(msg["args"]["key"],self.nodePred[2], self.nodeID):
                send_CMD = {
                    "cmd":ANSWER_RESP , 
                    "args":{
                        "key_data": msg["args"]["key"],
                        "resp":{
                            "IP": self.nodeIP_adress, 
                            "port": self.nodePort, 
                            "idNode":self.nodeID
                        }        
                    }
                }
                self.Send_Command((msg["args"]["host"]["IP"],msg["args"]["host"]["port"]),send_CMD)
            else:
                resp = self.Find_resp(msg["args"]["key"])
                if  msg["args"]["key"] == self.nodePred[2] or resp == self.nodeID:
                    self.Send_Command(self.nodePred[0:2],msg)
                else: 
                    self.Send_Command(resp[0:2],msg)


        elif  msg["cmd"] == GET:
            
            self.handle_get(msg)
        elif msg["cmd"] == ANSWER:
            if msg["args"]["Value_Exist"]:
                print("received value : " + str(msg["args"]["value"]))
            else:
                print("les données n'existent pas")

        elif msg["cmd"] == STATS:
            if self.nodeID == msg["args"]["source"]["idNode"]:
                print("--------statistiques--------")
                print("nombre de gets : "+ str(msg["args"]["NB_PUT"]))
                print("nombre de puts : "+ str(msg["args"]["NB_PUT"]))
                print("nombre de joins et requete intermediaire  : "+ str(msg["args"]["NB_JOIN"]))
            else:
                msg["args"]["NB_PUT"] += self.NB_PUT 
                msg["args"]["NB_PUT"] += self.NB_PUT 
                msg["args"]["NB_JOIN"] += self.NB_JOIN
                resp = self.Find_resp(msg["args"]["source"]["idNode"])
                self.Send_Command((resp[0:2]),msg)

        elif msg["cmd"] == PRINT:
            print("le noeud est resp des cles allant de "+str(self.nodeData[0])+ " jusqu'a "+str(self.nodeData[1]))
            print("liste des data: "+ str(self.nodeData[2]))        

        elif msg["cmd"] == "send_"+PUT:
            self.PUT_CMD(msg["args"]["key-data"],msg["args"]["value"])
        elif msg["cmd"] == "send_"+GET:
            self.GET_CMD(msg["args"]["key"])
        elif msg["cmd"] == "send_"+ STATS:
            self.get_stats()
        else:
            print("Unknown command")
        print("my data",self.TV)
        self.listen()
