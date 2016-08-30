'''
@author: antoine.herman
'''
import os
import subprocess
import csv
from pg.pgbasics import *
from pg.pgsqlite import SqliteConn


class PgSave():    
    '''
    Classe permettant de sauvegarder sous format sql ou dump à partir de commandes pg_dump
    '''
    
    def __init__(self, hote, base, utilisateur, mdp):
        self.hote = hote
        self.base = base
        self.utilisateur = utilisateur
        self.mdp = mdp

    def sauvegarder_base_format_sql(self, fichier):
        '''
        Sauvegarde la base sous format sql dans le nom de fichier spécifié. 
        Lors de la restauration, la base sera créée.        
        La commande pg_dump doit être disponible sur la machine.
        '''
        cmd = self._commande_pg_dump_base_entiere(fichier)
        return self.executer_commande_pg_dump(cmd)

    def sauvegarder_schemas_format_dump(self, fichier, schemas):
        '''
        Sauvegarde les schemas sous format dump dans le nom de fichier spécifié.        
        La commande pg_dump doit être disponible sur la machine.
        '''
        cmd = self._commande_pg_dump_schemas(fichier, schemas, format_dump=True)
        return self.executer_commande_pg_dump(cmd)

    def sauvegarder_schemas_format_sql(self, fichier, schemas):
        '''
        Sauvegarde les schemas sous format sql dans le nom de fichier spécifié.        
        La commande pg_dump doit être disponible sur la machine.
        '''
        cmd = self._commande_pg_dump_schemas(fichier, schemas, format_dump=False)
        return self.executer_commande_pg_dump(cmd)

    def sauvegarder_tables_format_dump(self, fichier, tables):
        '''
        Sauvegarde les tables sous format dump dans le nom de fichier spécifié.        
        La commande pg_dump doit être disponible sur la machine.
        '''
        cmd = self._commande_pg_dump_tables(fichier, tables, format_dump=True)
        return self.executer_commande_pg_dump(cmd)

    def sauvegarder_tables_format_sql(self, fichier, tables):
        '''
        Sauvegarde les tables sous format sql dans le nom de fichier spécifié.        
        La commande pg_dump doit être disponible sur la machine.
        '''
        cmd = self._commande_pg_dump_tables(fichier, tables, format_dump=False)
        return self.executer_commande_pg_dump(cmd)
    
    def executer_commande_pg_dump(self, cmd):
        '''
        Execute le process pg_dump
        '''
        if not self.verification_commande_pg_dump():
            raise Exception("La commande pg_dump n'existe pas.")
        print('-- DEBUT CREATION SAUVEGARDE:')        
        p = subprocess.run(cmd,input=self.mdp, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        if p.returncode == 0:
            print(p.stdout, p.stderr)
            print('-- FIN CREATION SAUVEGARDE')
        else:
            print('ECHEC SAUVEGARDE :', p.stderr)
        return p
    
    def verification_commande_pg_dump(self):
        cmd = ['pg_dump', '--version']
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        if p.returncode == 0 and p.stdout.startswith('pg_dump'):
            return True
        return False
        
    def _commande_pg_dump_racine(self, fichier, format_dump=False):
        cmd = ['pg_dump', 
               '-h', self.hote, 
               '-U', self.utilisateur, 
               '-v', '-O', '-W',
               '-f', fichier]
        if format_dump:
            cmd.append('-Fc')
        return cmd
    
    def _commande_pg_dump_base_entiere(self, fichier):
        cmd = self._commande_pg_dump_racine(fichier, format_dump=False)
        cmd += ['-C', self.base]
        return cmd
    
    def _commande_pg_dump_schemas(self, fichier, schemas, format_dump=False):
        cmd = self._commande_pg_dump_racine(fichier, format_dump)
        for schema in schemas:
            cmd += ['-n', schema]
        cmd.append(self.base)
        return cmd
    
    def _commande_pg_dump_tables(self, fichier, tables, format_dump=False):
        cmd = self._commande_pg_dump_racine(fichier, format_dump)
        for table in tables:
            cmd += ['-t', table]
        cmd.append(self.base)
        return cmd
    
class PgLoad():    
    '''
    Classe permettant de charger des données au format sql à partir de commandes psql
    '''
    
    def __init__(self, hote, base, utilisateur):
        self.hote = hote
        self.base = base
        self.utilisateur = utilisateur
    
    def charger_fichier_sql(self, fichier_sql):
        cmd = 'psql -h {0} -U {1} -d {2} -f "{3}"'.format(self.hote, self.utilisateur, self.base, fichier_sql)
        os.system(cmd)
        
    def charger_fichier_sql_dans_nouvelle_base(self, fichier_sql, postgis = True):
        self.effacer_base()
        self.creer_nouvelle_base()
        if postgis :
            self.creer_extensions_postgis()
        self.charger_fichier_sql(fichier_sql)
    
    def effacer_base(self):
        cmd = 'psql -h {0} -U {1} -c "DROP DATABASE IF EXISTS {2};"'.format(self.hote, self.utilisateur, self.base)
        os.system(cmd)
    
    def creer_nouvelle_base(self):
        cmd = 'psql -h {0} -U {1} -c "CREATE DATABASE {2};"'.format(self.hote, self.utilisateur, self.base)
        os.system(cmd)
    
    def creer_extensions_postgis(self):
        cmd = 'psql -h {0} -U {1} -d {2} -c "CREATE EXTENSION postgis; CREATE EXTENSION postgis_topology;"'.format(self.hote, self.utilisateur, self.base)
        os.system(cmd)   
     

class PgExport(PgOutils):
    '''
    Classe permettant d'exporter des tables PostgreSQL sous d'autres formats (sqlite, csv, etc.)
    '''
    
    VUE_TEMPORAIRE = 'public.temporaire'
    
    def __init__(self, hote, base, port, utilisateur, motdepasse):
        super().__init__(hote, base, port, utilisateur, motdepasse) 
    
    def _creer_vue_temporaire(self, select_sql):
        '''
        Crée une vue temporaire avec la requête SELECT spécifiée
        '''
        requete_vue = '''DROP VIEW IF EXISTS {1};
        CREATE VIEW {1} AS {0}'''.format(select_sql, self.VUE_TEMPORAIRE)
        return self.execution(requete_vue)
    
    def _recuperer_donnees_table_pg(self, schema, table, limit = None):
        '''
        Renvoie les données de la table PostgreSQL spécifiée dans une liste de tuples
        '''
        if limit is None:
            select = 'SELECT * FROM {0}.{1};'.format(schema, table)
        else:
            select = 'SELECT * FROM {0}.{1} LIMIT {2};'.format(schema, table, limit)
        return self.execution_et_recuperation(select)

    '''
    EXPORT SQLITE
    '''
    
    def exporter_table_vers_sqlite(self, schema, table, nom_bdd_sqlite, nom_table_sqlite, recreer_table = True, limit = None):
        '''
        Insére des données issues d'une table PostgreSQL dans la table d'une base sqlite
        
        Par défaut, la table sqlite est créée (ou recréée) si elle existe.
        Si recreer_table est False, les données sont insérées à la table sqlite existante.
        Si une valeur entière n est spécifiée pour limit, seules les n premières lignes de la table sont exportées.   
        '''
        champs = self.lister_champs(schema, table)
        if recreer_table:
            self._creer_table_sqlite(nom_bdd_sqlite, nom_table_sqlite, champs)
        donnees = self._recuperer_donnees_table_pg(schema, table, limit)
        self._inserer_donnees_dans_sqlite(donnees, nom_bdd_sqlite, nom_table_sqlite)
    
    def exporter_requete_vers_sqlite(self, sql, nom_bdd_sqlite, nom_table_sqlite, recreer_table = True, limit = None):
        '''
        Insére des données issues d'une requête dans la table d'une base sqlite
        
        Par défaut, la table sqlite est créée (ou recréée) si elle existe.
        Si recreer_table est False, les données sont insérées à la table sqlite existante.
        '''
        schema, table = self.VUE_TEMPORAIRE.split('.')
        reussite, nb = self._creer_vue_temporaire(sql)
        if reussite:
            self.exporter_table_vers_sqlite(schema, table, nom_bdd_sqlite, nom_table_sqlite, champs, recreer_table, limit)        
    
    def _creer_table_sqlite(self, nom_bdd_sqlite, nom_table_sqlite, champs):
        champs_pour_requete = ', '.join([nom + ' ' + type for position, nom, type in champs]) 
        drop_table = 'DROP TABLE IF EXISTS {0};'.format(nom_table_sqlite)
        create_table = 'CREATE TABLE {0} ({1});'.format(nom_table_sqlite, champs_pour_requete)
        sqlite = SqliteConn(nom_bdd_sqlite)
        for sql in [drop_table, create_table]:
            sqlite.execute_commit(sql)
           
    def _inserer_donnees_dans_sqlite(self, donnees, nom_bdd_sqlite, nom_table_sqlite):
        sqlite = SqliteConn(nom_bdd_sqlite)
        insert = 'INSERT INTO {0} VALUES ({1});'.format(nom_table_sqlite, ', '.join(['?']*len(donnees[0])))
        sqlite.execute_many(insert, donnees)
    
    '''
    EXPORT CSV
    '''
    
    def exporter_table_vers_csv(self, schema, table, fichier_csv, delimiteur = '|', limit = None):
        champs = [nom for position, nom, type in self.lister_champs(schema, table)]
        donnees = self._recuperer_donnees_table_pg(schema, table, limit=limit)
        self._ecrire_dans_csv(fichier_csv, champs, donnees, delimiteur)
    
    def exporter_requete_vers_csv(self, sql, fichier_csv, delimiteur = '|', limit = None):
        schema, table = self.VUE_TEMPORAIRE.split('.')
        reussite, nb = self._creer_vue_temporaire(sql)
        if reussite:
            self.exporter_table_vers_csv(schema, table, fichier_csv, delimiteur, limit = limit)
        
    def _ecrire_dans_csv(self, fichier_csv, champs, donnees, delimiteur):
        with open(fichier_csv, 'wt',encoding ='utf-8') as fichier:
            csv_writer = csv.writer(fichier, delimiter = delimiteur)
            csv_writer.writerow(champs)
            for ligne in donnees:
                csv_writer.writerow(ligne)
    
    '''
    EXPORT HTML
    '''
                
    def exporter_table_vers_html(self, schema, table, fichier_html, support_html = None, limit = None):
        support = self._definir_support_html(support = support_html)
        table_html = self._table_pg_vers_table_html(schema, table, limit)
        html = support.format(table_html)
        self._ecrire_dans_fichier_html(html, fichier_html)
    
    def exporter_requete_vers_html(self, sql, fichier_html, support_html = None, limit = None):
        support = self._definir_support_html(support = support_html)
        table_html = self._requete_vers_table_html(sql, limit)
        html = support.format(table_html)
        self._ecrire_dans_fichier_html(html, fichier_html)
    
    def _definir_support_html(self, support = None):
        if not support:
            support = '''
<!DOCTYPE html>
<html lang="fr">
  <head>
    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Export html</title>
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap.min.css" integrity="sha384-1q8mTJOASx8j1Au+a5WDVnPi2lkFfwwEAa8hDDdjZlpLegxhjVME1fgjWPGmkzs7" crossorigin="anonymous">
  </head>
  <body>
      <div class="container">
        {0}
      </div>
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/1.11.3/jquery.min.js"></script>
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/js/bootstrap.min.js" integrity="sha384-0mSbJDEHialfmuBBQP6A4Qrprq5OVfW37PRR3j5ELqxss1yVqOtnepnHVP9aJ7xS" crossorigin="anonymous"></script>
  </body>
</html>
            '''
        return support
    
    def _table_pg_vers_table_html(self, schema, table, limit = None):
        '''
        Renvoie le resultat de la requête sous forme d'un tableau html
        '''
        champs = [nom for position, nom, type in self.lister_champs(schema, table)]
        donnees = self._recuperer_donnees_table_pg(schema, table, limit = limit)
        return self._ecrire_tableau_html(champs, donnees)
    
    def _requete_vers_table_html(self, sql, limit = None):
        '''
        Renvoie le resultat de la requête sous forme d'un tableau html
        '''
        schema, table = self.VUE_TEMPORAIRE.split('.')
        reussite, nb = self._creer_vue_temporaire(sql)
        if reussite:
            return self._table_pg_vers_table_html(schema, table, limit = limit)        
    
    def _ecrire_tableau_html(self, champs, donnees):
        tableau_html = '<table class = "table table-condensed table-striped">\n'
        # entete
        tableau_html += '\t<tr>\n'
        for nom_champ in champs:                
            tableau_html += '\t\t<th>' + str(nom_champ) + '</th>\n'
        tableau_html += '\t</tr>\n'
        # corps
        for ligne in donnees:
            tableau_html += '\t<tr>\n'            
            for elt in ligne:
                tableau_html += '\t\t<td>' + str(elt) + '</td>\n'
            tableau_html += '\t</tr>\n'        
        tableau_html += '</table>'
        return tableau_html
    
    def _ecrire_dans_fichier_html(self, html, fichier_html):
        with open(fichier_html, 'wt',encoding="utf-8") as f:
            f.write(html)
            
class PgImport(PgOutils):
    
    '''
    Classe permettant d'importer des tables dans PostgreSQL depuis d'autres formats (sqlite, csv, etc.)
    '''
        
    def __init__(self, hote, base, port, utilisateur, motdepasse):
        super().__init__(hote, base, port, utilisateur, motdepasse)
    
    def importer_table_depuis_sqlite(self, fichier_sqlite, table_sqlite, schema, table):
        s = SqliteConn(fichier_sqlite)
        self._creer_table_pg(fichier_sqlite, table_sqlite, schema, table, s)        
        donnees = self._recuperer_donnees_sqlite(table_sqlite, s)
        self._inserer_donnees_dans_postgres(donnees, schema, table)
    
    def _creer_table_pg(self, fichier_sqlite, table_sqlite, schema, table, s):
        self.creer_schema_si_inexistant(schema)
        self.search_path(schema)        
        create_table = s.recuperer_requete_creation_table(table_sqlite)
        create_table = create_table.replace('"' + table_sqlite + '"', table)
        create_table = create_table.replace('AUTOINCREMENT', '')
        self.effacer_table(schema, table)
        self.execution_et_ecriture_script(create_table)
    
    def _recuperer_donnees_sqlite(self, table_sqlite, s):
        select = '''SELECT * FROM {0};'''.format(table_sqlite)
        donnees = s.execute_recupere(select)
        return donnees

    def _inserer_donnees_dans_postgres(self, donnees, schema, table):
        insert = 'INSERT INTO {0} VALUES ({1});'.format(schema + '.' + table, ', '.join(['%s']*len(donnees[0])))
        self.execution_multiple(insert, donnees)
