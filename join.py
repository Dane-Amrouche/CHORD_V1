#!/usr/bin/env python3
import sys
import socket
import node
import random


'''
Lancement du programme chord(comme précisé dans le sujet):
./join.py <port> - commande pour la premiere insertion
./join.py <NodeID> <ip> <Point d'entree> -- ecoute sur le port NodeID
'''

IP = "127.0.0.1"
NODE_ID = None
#NODE_ID= 125 random.randint(0, 255)
if len(sys.argv) == 2:
    #premiere insertion
    NODE_ID = int(sys.argv[1])
    print("l'Id du noeud",NODE_ID)
    node = node.Node(False,NODE_ID, IP,int(sys.argv[1]))
elif len(sys.argv) == 4:
    NODE_ID = int(sys.argv[1])
    print("l'Id du noeud ",NODE_ID)
    # les autre insertion (se Joindre a un reseau non vide ) 
    node = node.Node(True, NODE_ID,IP, int(sys.argv[1]),(sys.argv[2],int(sys.argv[3])))
else:
    print("INVALID PARAMETERS")




