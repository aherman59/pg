'''
@author: antoine.herman
'''

import sqlite3
   
class SqliteConn():
    '''
    Classe permettant de se connecter à une base sqlite, à y effectuer des requêtes
    '''

    def __init__(self, nom_bdd, connection_directe = True):
        '''
        Constructeur qui prend les paramètres de connexion suivant:
        - nom_bdd
        
        Par défaut, se connecte directement à la base. Pour ne pas se connecter directement, mettre connection_directe à False.
        '''
        self.connection = None      
        
        self.conn_actif = False
        
        self.nom_bdd = nom_bdd
        if connection_directe:
            self.connection_sqlite()     
        
            
    def connection_sqlite(self):
        '''
        Connexion à la base sqlite.
        ''' 
        try:  
            self.connection = sqlite3.connect(self.nom_bdd)
            self.conn_actif = True            
        except Exception as e:
            print('Connexion impossible')
            self.conn_actif = False

        
    def deconnection_sqlite(self):
        '''
        Deconnexion de la base sqlite si la connexion était établie
        '''
        try:            
            self.connection.close()
            self.conn_actif = False
        except Exception as e:
            pass
        

    def execute_commit(self, sql):
        '''
        Execute la requete sql et la soumet au serveur, 
        Renvoie également le nombre de lignes modifiés en fonction du type de requete.
        '''
        nb_ligne_affectee = -1
        if not self.conn_actif:
            self.connection_sqlite()
        curseur = self.connection.cursor() 
        curseur.execute(sql)
        nb_ligne_affectee = curseur.rowcount            
        self.connection.commit()
        return nb_ligne_affectee
    
    def execute_many(self, sql, donnees):
        '''
        Execute la requete sql et la soumet au serveur, 
        Renvoie également le nombre de lignes modifiés en fonction du type de requete.
        '''
        nb_ligne_affectee = -1
        if not self.conn_actif:
            self.connection_sqlite()
        curseur = self.connection.cursor() 
        curseur.executemany(sql, donnees)
        nb_ligne_affectee = curseur.rowcount            
        self.connection.commit()
        return nb_ligne_affectee    
    
    def execute_recupere(self, sql):
        '''
        Execute la requete sql (SELECT) et renvoie le resultat dans une liste de tuple
        '''
        if not self.conn_actif:
            self.connection_sqlite()
        curseur = self.connection.cursor()
        curseur.execute(sql)        
        return curseur.fetchall()
    
    def recuperer_requete_creation_table(self, table):
        return self.execute_recupere('''SELECT sql FROM sqlite_master WHERE tbl_name = '{0}';'''.format(table))[0][0]
