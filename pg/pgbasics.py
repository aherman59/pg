'''
@author: antoine.herman
'''

import psycopg2
import csv
import os
import sys
import inspect
from datetime import datetime
from random import randint


'''

DECORATEURS

'''

def requete_sql(fonction):    
    def interne(self, *args, **kwargs):        
        requete = self.requete_sql[fonction.__name__.upper()].format(*args)
        reussite, nb = self.execution_et_ecriture_script(requete)
        return reussite, nb
    return interne

def requete_sql_avec_modification_args(fonction):    
    def interne(self, *args, **kwargs):
        args = fonction(self, *args, **kwargs)        
        requete = self.requete_sql[fonction.__name__.upper()].format(*args)
        reussite, nb = self.execution_et_ecriture_script(requete)
        return reussite, nb
    return interne

def select_sql(fonction):    
    def interne(self, *args, **kwargs):
        requete = self.requete_sql[fonction.__name__.upper()].format(*args)
        resultat = self.execution_et_recuperation(requete)
        return resultat
    return interne

def select_sql_avec_modification_args(fonction):    
    def interne(self, *args, **kwargs):
        args = fonction(self, *args, **kwargs)        
        requete = self.requete_sql[fonction.__name__.upper()].format(*args)
        resultat = self.execution_et_recuperation(requete)
        return resultat
    return interne

def select_sql_champ_unique(fonction):    
    def interne(self, *args, **kwargs):
        requete = self.requete_sql[fonction.__name__.upper()].format(*args)
        resultat = self.execution_et_recuperation(requete)
        return [r[0] for r in resultat] 
    return interne

def select_sql_valeur_unique(fonction):    
    def interne(self, *args, **kwargs):
        requete = self.requete_sql[fonction.__name__.upper()].format(*args)
        resultat = self.execution_et_recuperation(requete)
        return resultat[0][0] 
    return interne

'''

CLASSES

'''


class PgOutils():
    '''
    Classe permettant de réaliser des opérations simples sous une base de données PostgreSQL
    '''
    
    START_SCRIPT = '''
--
-- Début d'exécution le {0:%d}/{0:%m}/{0:%Y} à {0:%H}:{0:%M}:{0:%S}
--

'''

    END_SCRIPT = '''
--
-- Fin d'exécution le {0:%d}/{0:%m}/{0:%Y} à {0:%H}:{0:%M}:{0:%S}
--

'''


    def __init__(self, hote, base, port, utilisateur, motdepasse, script = None):
        '''
        Constructeur
        Se connecte directement à la base de données
        Toutes les requêtes exécutées à partir de l'objet instancié sont écrites dans le fichier script.
        '''
        self.pgconn = PgConn(hote, base, port, utilisateur, motdepasse)
        self.script = script
        
        self.requete_sql = {}
        for classe in inspect.getmro(self.__class__):
            if classe != type(object()):
                try:
                    repertoire_module = os.path.dirname(os.path.abspath(inspect.getfile(classe)))
                    self.charger_requete_sql_depuis(os.path.join(repertoire_module, classe.__name__.lower() + '.sql'))
                except (OSError, IOError) as e:
                    print(e)
            
    def charger_requete_sql_depuis(self, fichier):
        with open(fichier, 'rt', encoding = 'UTF-8') as f:
            lignes = f.readlines()
            clef = None
            for ligne in lignes:
                if ligne.strip().startswith('##'):
                    clef = ligne.strip()[2:].strip()
                    self.requete_sql[clef] = ''
                    continue
                self.requete_sql[clef] = self.requete_sql[clef] + ligne
    
    def connecter(self):
        '''
        Connection de l'objet à la base
        '''
        self.pgconn.connection_postgres()
    
    def deconnecter(self):
        '''
        Déconnexion de l'objet à la base de données        
        '''
        self.pgconn.deconnection_postgres()
        
    def execution(self, sql, max_tentative = 3):
        '''
        Se connecte
        Tente l'execution d'une requete sql (3 fois par défaut)
        Se déconnecte
        Renvoie True si la requete a réussi ainsi que le nombre de lignes modifiées ou renvoyées en fonction du type de requête
        '''
        reussite = False
        nb_lignes_modifiees = -1 
        tentative = 1
        while(tentative <= max_tentative and not reussite):
            try:
                nb_lignes_modifiees = self.pgconn.execute_commit(sql)                
                print(sql)                
                reussite = True                
            except Exception as e:
                print(e)
                print('Tentative ' + str(tentative) + ' échouée...')                
                if tentative == max_tentative:
                    print('REQUETE EN ECHEC : ')
                    print(sql)
                    #sys.exit('FIN PREMATUREE - TRAITEMENT NON ABOUTI')
                tentative += 1
        return reussite, nb_lignes_modifiees
    
    def execution_multiple(self, sql, donnees, max_tentative = 3):
        '''
        Se connecte
        Tente l'execution d'une requete multiple sql (3 fois par défaut)
        Se déconnecte
        Renvoie True si la requete a réussi ainsi que le nombre de lignes modifiées ou renvoyées en fonction du type de requête
        '''
        reussite = False
        nb_lignes_modifiees = -1 
        tentative = 1
        while(tentative <= max_tentative and not reussite):
            try:
                nb_lignes_modifiees = self.pgconn.execute_many(sql, donnees)                
                print(sql)                
                reussite = True                
            except Exception as e:
                print(e)
                print('Tentative ' + str(tentative) + ' échouée...')                
                if tentative == max_tentative:
                    print('REQUETE EN ECHEC : ')
                    print(sql)
                    #sys.exit('FIN PREMATUREE - TRAITEMENT NON ABOUTI')
                tentative += 1
        return reussite, nb_lignes_modifiees
    
    def execution_et_recuperation(self, sql, max_tentative = 3):
        '''
        Se connecte
        Tente l'execution d'une requete sql (3 fois par défaut)
        Se déconnecte
        Renvoie le résultat de la requete sous forme d'une liste de tuple
        '''
        reussite = False
        tentative = 1
        while(tentative <= max_tentative and not reussite):
            try:
                resultat = self.pgconn.execute_recupere(sql)
                self.redaction_script(self.script, sql, False)                
                print(sql)                
                reussite = True                
            except Exception as e:
                print(e)                
                print('Tentative ' + str(tentative) + ' échouée...')
                if tentative == max_tentative:
                    print('REQUETE EN ECHEC : ')
                    print(sql)
                    #sys.exit('FIN PREMATUREE - TRAITEMENT NON ABOUTI')
                tentative += 1
        return resultat
        
    def execution_et_ecriture_script(self, sql, max_tentative = 3):
        '''
        Se connecte
        Tente l'execution d'une requete sql (3 fois par défaut)
        Se déconnecte
        Ecrit la requete correspondante dans le fichier script défini
        Renvoie True si la requete a réussi ainsi que le nombre de lignes modifiées ou renvoyées en fonction du type de requête
        '''
        reussite = False
        nb_lignes_modifiees = -1 
        tentative = 1
        while(tentative <= max_tentative and not reussite):
            try:
                nb_lignes_modifiees = self.pgconn.execute_commit(sql)
                self.redaction_script(self.script, sql, False)
                print(sql)                
                reussite = True                
            except Exception as e:
                print(e)
                print('Tentative ' + str(tentative) + ' échouée...')
                if tentative == max_tentative:
                    print('REQUETE EN ECHEC : ')
                    print(sql)  
                    #sys.exit('FIN PREMATUREE - TRAITEMENT NON ABOUTI')
                tentative += 1
        return reussite, nb_lignes_modifiees
    
    def redaction_script(self, file, sql, effacement):
        '''
        Ecrit la requête sql dans le fichier défini.
        Efface le contenu du fichier si effacement est à True
        '''
        if file:
            mode = 'w' if effacement else 'a'
            try:
                with open(file, mode, encoding = 'utf-8') as f:
                    f.write(sql)
            except (IOError, OSError) as e:
                pass
    
    def start_script(self):
        '''
        Ecrit le début d'exécution du script.
        '''
        self.redaction_script(self.script, self.START_SCRIPT.format(datetime.now()), True)
    
    def end_script(self):
        '''
        Ecrit la fin d'exécution du script.
        '''
        self.redaction_script(self.script, self.END_SCRIPT.format(datetime.now()), False)
    
    @requete_sql
    def search_path(self, schema):
        '''
        Modifie le search_path pour accéder au schema défini sans devoir le spécifier
        '''
        pass
    
    @requete_sql    
    def effacer_schema(self, schema):
        '''
        Supprime le schéma de la base de données s'il existe en CASCADE.
        '''
        pass
    
    def effacer_schemas(self, schemas):        
        '''
        Supprime les schemas de la base de données en CASCADE.             
        '''        
        for schema in schemas:
            reussite, nb = self.effacer_schema(schema)
            if not reussite:
                return False, -1
        return True, len(schemas)
    
    def effacer_schemas_commencant_par(self, prefixe):
        '''
        Supprime les schemas commencant par le préfixe spécifié de la base de données en CASCADE.
        '''
        schemas = self.lister_schemas_commencant_par(prefixe)
        return self.effacer_schemas(schemas)
    
    @requete_sql
    def creer_schema(self, schema):
        '''
        Crée le schéma.
        '''
        pass
    
    @requete_sql
    def creer_schema_si_inexistant(self, schema):
        '''
        Crée le schema s'il n'existe pas
        '''
        pass
    
    def effacer_et_creer_schema(self, schema):
        '''
        Supprime le schéma s'il existe et le recrée.
        '''
        reussite, nb = self.effacer_schema(schema)
        if reussite:
            return self.creer_schema(schema)
    
    def effacer_et_creer_schemas(self, schemas):
        '''
        Supprime et recrée les schémas.
        '''
        for schema in schemas:
            reussite, nb = self.effacer_et_creer_schema(schema)
            if not reussite:
                return False, -1
        return True, len(schemas)
    
    @requete_sql
    def effacer_et_creer_sequence(self, schema, sequence):
        '''
        Supprime la séquence si elle existe et la recrée.
        '''
        pass
    
    @requete_sql
    def effacer_table(self, schema, table):
        '''
        Supprime la table dans le schéma si elle existe.
        '''
        pass    
    
    def effacer_tables(self, schema, tables):
        '''
        Supprime les tables listées du schéma.
        '''
        for table in tables:
            reussite, nb = self.effacer_table(schema, table)
            if not reussite:
                return False, -1
        return True, len(tables)            
    
    def effacer_tables_commencant_par(self, schema, prefixe):
        '''
        Supprime les tables du schéma commençant par le prefixe specifié.
        '''
        tables = self.lister_tables_commencant_par(schema, prefixe)
        return self.effacer_tables(schema, tables)
    
    @requete_sql
    def renommer_table(self, schema, table, nveau_nom_table):
        pass
    
    @select_sql_champ_unique
    def lister_schemas(self):
        '''
        Renvoie, sous forme de liste, l'ensemble des schémas de la base de données
        '''
        pass
    
    @select_sql_champ_unique
    def lister_schemas_commencant_par(self, prefixe):
        '''
        Renvoie, sous forme de liste, les schémas de la base de données commençant par le préfixe défini
        '''
        pass
    
    @select_sql_champ_unique
    def lister_tables(self, schema):
        '''
        Renvoie, sous forme de liste, les noms des tables du schéma
        '''
        pass
    
    @select_sql_champ_unique
    def lister_tables_commencant_par(self, schema, prefixe):
        '''
        Renvoie, sous forme de liste, les noms des tables du schéma commençant par le préfixe défini
        '''
        pass
    
    @select_sql
    def lister_champs(self, schema, table):
        '''
        Renvoie la liste des champs présents dans la table sous forme de tuples (position, nom, type_de_variable)
        '''
        pass
    
    @requete_sql_avec_modification_args
    def ajouter_clef_primaire(self, schema, table, champs):
        '''
        Ajoute une clef primaire à la table composée de l'ensemble des champs présents dans la liste.
        '''
        return schema, table, ', '.join(champs)
    
    @requete_sql_avec_modification_args
    def ajouter_contrainte_unicite(self, schema, table, champs):
        '''
        Ajoute une contrainte d'unicité à la table composée de l'ensemble des champs présents dans la liste.
        '''
        return schema, table, ', '.join(champs)

    @requete_sql
    def ajouter_contrainte_check(self, schema, table, expression):
        '''
        Ajoute une contrainte de type check à la table avec l'expression définie.
        '''
        pass
    
    @requete_sql_avec_modification_args
    def ajouter_commentaire_sur_champ(self, schema, table, champ, commentaire):
        '''
        Ajout un commentaire au champ de la table.
        Renvoie True si le commentaire a bien été créé.
        '''
        return schema, table, champ, commentaire.replace('\'', '\'\'')
    
    @requete_sql_avec_modification_args
    def ajouter_commentaire_sur_table(self, schema, table, commentaire):
        '''
        Ajout un commentaire à la table.
        Renvoie True si le commentaire a bien été créé.
        '''
        return schema, table, commentaire.replace('\'', '\'\'')
    
    @requete_sql
    def copier_table(self, schema_initial, table_initiale, schema_final, table_finale):
        '''
        Copie les données d'une table initiale dans une nouvelle table.
        Supprime la table de destination si celle-ci existe déjà.
        Renvoie True si la table a bien été créé.
        '''
        pass

    @requete_sql
    def changer_schema(self, schema_initial, table, schema_final):
        '''
        Change le schema de la table
        '''
        pass
    
    @select_sql_valeur_unique
    def compter(self, schema, table):
        '''
        Compter le nombre de ligne de la table
        '''
        pass
    
    @select_sql_champ_unique
    def lister_valeurs(self, champ, schema, table):
        '''
        Renvoie la liste des valeurs contenues pour le champ dans la table
        '''
        pass
    
    @select_sql_champ_unique
    def lister_valeurs_distinctes(self, champ, schema, table):
        '''
        Renvoie la liste des valeurs distinctes contenues pour le champ dans la table
        '''
        pass
    
    @requete_sql
    def creer_extension_fdw(self):
        '''
        Crée l'extension fdw de postgresql
        '''
        pass
    
    @requete_sql
    def creer_serveur_distant_fdw(self, hote_distant, base_distante, port, nom_serveur_distant):
        '''
        Crée un serveur distant pour le foreign data wrapper
        '''
        pass
    
    @requete_sql
    def effacer_serveur_distant_fdw(self, nom_serveur_distant):
        '''
        Efface un serveur distant en cascade s'il existe pour le foreign data wrapper
        '''
        pass 
    
    @requete_sql
    def creer_user_mapping_pour_serveur_distant_fdw(self, utilisateur, nom_serveur_distant, motdepasse):
        '''
        Créer un user mapping pour le serveur distant
        '''
        pass
    
    @requete_sql
    def effacer_user_mapping_pour_serveur_distant_fdw(self, utilisateur, nom_serveur_distant):
        '''
        Efface un user mapping pour le serveur distant s'il existe
        '''
        pass
    
    def mettre_en_place_serveur_distant_fdw(self, hote_distant, base_distante, port, utilisateur, motdepasse, nom_serveur_distant):
        '''
        Prépare un serveur distant fdw et crée un user mapping associé.
        '''
        valid, nb = self.creer_extension_fdw()
        valid2, nb = self.effacer_serveur_distant_fdw(nom_serveur_distant)        
        valid3, nb = self.creer_serveur_distant_fdw(hote_distant, base_distante, port, nom_serveur_distant)
        valid4, nb = self.effacer_user_mapping_pour_serveur_distant_fdw(utilisateur, nom_serveur_distant) 
        valid5, nb = self.creer_user_mapping_pour_serveur_distant_fdw(utilisateur, nom_serveur_distant, motdepasse)
        return (True, 1,) if valid and valid2 and valid3 and valid4 and valid5 else (False, -1,)
    
    @requete_sql
    def effacer_table_etrangere(self, schema, table):
        pass
    
    @requete_sql_avec_modification_args
    def creer_table_etrangere(self, schema, table, schema_distant, table_distante, champs, nom_serveur):
        '''
        Efface puis crée une table étrangère à partir de la table distante
        la variable champ doit avoir le formalisme prévue par le renvoi de la méthode lister_champs()
        '''
        self.effacer_table_etrangere(schema, table)
        champs_format = ',\n'.join([c[1] + ' ' + c[2] for c in champs])
        return schema, table, schema_distant, table_distante, champs_format, nom_serveur
    
    def copier_table_distante(self, hote_distant, base_distante, port, utilisateur, motdepasse, schema_initial, table_initiale, schema_final, table_finale):
        '''
        Copie une table d'une base de données distante dans la base locale
        '''
        nom_serveur_temporaire = 'serveur_tmp' + str(randint(1,100000))
        valid, nb = self.mettre_en_place_serveur_distant_fdw(hote_distant, base_distante, port, utilisateur, motdepasse, nom_serveur_temporaire)
        pgoutils_distant = PgOutils(hote_distant, base_distante, port, utilisateur, motdepasse, self.script)
        champs = pgoutils_distant.lister_champs(schema_initial, table_initiale)
        valid2, nb = self.creer_table_etrangere(schema_final, table_finale + '_fdw', schema_initial, table_initiale, champs, nom_serveur_temporaire) 
        valid3, nb = self.copier_table(schema_final, table_finale + '_fdw', schema_final, table_finale)
        valid4, nb = self.effacer_table_etrangere(schema_final, table_finale + '_fdw')
        valid5, nb = self.effacer_user_mapping_pour_serveur_distant_fdw(utilisateur, nom_serveur_temporaire)
        valid6, nb = self.effacer_serveur_distant_fdw(nom_serveur_temporaire)
        self.deconnecter() # permettre d'effacer la connexion postgres_fdw qui sinon restera en attente inutilement
        self.connecter()  # reconnexion dans la foulée. 
        pgoutils_distant.deconnecter()
        return (True, 1,) if valid and valid2 and valid3 and valid4 and valid5 and valid6 else (False, -1,)
    
    @requete_sql
    def creer_fonction_array_supprimer_null(self):
        pass
    
    @requete_sql
    def creer_fonction_pgcd(self):
        pass
    
    @requete_sql
    def creer_fonction_convertir_date(self):
        pass

    @requete_sql
    def creer_fonction_aggregat_fusion_array(self):
        pass
    
    @requete_sql
    def creer_fonction_array_sort_unique(self):
        pass
    
    @requete_sql
    def creer_fonction_aggregat_first(self):
        pass

class PgConn():
    '''
    Classe permettant de se connecter à une base Postgresql, à y effectuer des requêtes et à y importer des fichiers csv
    '''

    def __init__(self, hote, base, port, utilisateur, motdepasse, connection_directe = True):
        '''
        Constructeur qui prend les paramètres de connexion suivant:
        - hote
        - base
        - utilisateur
        - motdepasse
        
        Par défaut, se connecte directement à la base. Pour ne pas se connecter directement, mettre connection_directe à False.
        '''
        self.connection = None        
        
        self.conn_actif = False
        
        self.hote = hote
        self.base = base
        self.utilisateur = utilisateur
        self.motdepasse = motdepasse
        self.port = port
        if connection_directe:
            self.connection_postgres()     
        
            
    def connection_postgres(self, client_encoding = 'UTF-8'):
        '''
        Connexion à la base PostgreSQL via les paramètres définis lors de la création de l'instance de la classe.
        
        L'encodage du client est considéré par défaut comme du UTF-8. Il peut etre modifier par l'argument client_encoding.        
        
        ''' 
        try:  
            self.connection = psycopg2.connect(host=self.hote, database=self.base, user=self.utilisateur, password=self.motdepasse, port=self.port)
            self.connection.set_client_encoding(client_encoding)            
            self.conn_actif = True            
        except Exception as e:
            print('Connexion impossible : ', str(e))
            self.conn_actif = False

        
    def deconnection_postgres(self):
        '''
        Deconnexion de la base PostgreSQL si la connexion était établie
        '''
        try:            
            self.connection.close()
            self.conn_actif = False
        except Exception as e:
            print('Problème de déconnexion : ',  str(e))
        

    def execute_commit(self, sql):
        '''
        Execute la requete sql et la soumet au serveur, 
        Renvoie également le nombre de lignes modifiés en fonction du type de requete.
        '''
        nb_ligne_affectee = -1
        if not self.conn_actif:
            self.connection_postgres()
        with self.connection.cursor() as curseur: 
            curseur.execute(sql)
            nb_ligne_affectee = curseur.rowcount            
        self.connection.commit()
        return nb_ligne_affectee
    
    
    def execute_recupere(self, sql):
        '''
        Execute la requete sql (SELECT) et renvoie le resultat dans une liste de tuple
        '''
        if not self.conn_actif:
            self.connection_postgres()
        with self.connection.cursor() as curseur:
            curseur.execute(sql)        
            return curseur.fetchall()
    
    def execute_many(self, sql, donnees):
        '''
        Execute la requete sql et la soumet au serveur pour chacun des éléments de la liste de tuple donnees, 
        Renvoie également le nombre de lignes modifiés en fonction du type de requete.
        '''
        nb_ligne_affectee = -1
        if not self.conn_actif:
            self.connection_postgres()
        with self.connection.cursor() as curseur: 
            curseur.executemany(sql, donnees)
            nb_ligne_affectee = curseur.rowcount            
        self.connection.commit()
        return nb_ligne_affectee

    def copy_from_csv(self, fichier_csv, separateur, table, entete):
        """
            Copie le fichier csv encodé en UTF-8 dans la table définie de la PostgreSQL.
            
            Si entete est à True, la première ligne sera ignorée.
            
        """ 
        lignes = self._lire_lignes_csv(fichier_csv, separateur)
        self._ecrire_lignes_dans_csv(lignes, 'tmp', entete)
        
        if not self.conn_actif:
            self.connection_postgres()
        
        with open('tmp','r',encoding = 'utf-8') as donnees:
            with self.connection.cursor() as curseur:
                curseur.copy_from(donnees, table, separateur, '')            
            self.connection.commit()
        
        if os.path.exists('tmp'):
            os.remove('tmp')
        
        return True                  


    def _lire_lignes_csv(self, fichier_csv, separateur):
        """ Générateur qui renvoie les lignes d'un fichier csv encodé en UTF-8"""
        with open(fichier_csv, 'r', encoding = 'utf-8') as f:
            for ligne in f:
                yield ligne
                
    def _ecrire_lignes_dans_csv(self, lignes, fichier_sortie, entete):
        ''' Ecriture en UTF-8 dans le fichier de sortie des lignes en omettant l'entete si sa valeur est à True'''
        with open(fichier_sortie, 'wt', encoding = 'utf-8') as f:
            if entete:
                next(lignes)
            for ligne in lignes:
                print(ligne, end='', file=f)

#eof
    