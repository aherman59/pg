'''
@author: antoine.herman
'''
import unittest
import psycopg2
import os
from pg.pgbasics import *

hote = 'localhost'
bdd = 'test_pg'
utilisateur = 'postgres'
mdp = 'cerema59'
port = '5432'

class TestPgConn(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        os.system('''psql -h {0} -p {1} -U {2} -c "CREATE DATABASE {3};"'''.format(hote, port, utilisateur, bdd))
    
    @classmethod
    def tearDownClass(cls):
        os.system('''psql -h {0} -p {1} -U {2} -c "DROP DATABASE {3};"'''.format(hote, port, utilisateur, bdd))
    
    def test_une_nouvelle_instance_de_pg_conn_se_connecte_automatiquement_a_la_base(self):
        pgconn = PgConn(hote, bdd, port, utilisateur, mdp)
        self.assertEqual(pgconn.hote, hote)
        self.assertEqual(pgconn.base, bdd)
        self.assertEqual(pgconn.port, port)
        self.assertEqual(pgconn.utilisateur, utilisateur)
        self.assertEqual(pgconn.motdepasse, mdp)
        self.assertEqual(pgconn.connection.closed, 0) # connexion ouverte
        self.assertTrue(pgconn.conn_actif)
    
    def test_une_nouvelle_instance_de_pg_conn_ne_se_connecte_pas_a_la_base_si_connection_directe_est_False(self):
        pgconn = PgConn(hote, bdd, port, utilisateur, mdp, connection_directe=False)
        self.assertEqual(pgconn.hote, hote)
        self.assertEqual(pgconn.base, bdd)
        self.assertEqual(pgconn.port, port)
        self.assertEqual(pgconn.utilisateur, utilisateur)
        self.assertEqual(pgconn.motdepasse, mdp)
        self.assertEqual(pgconn.connection, None)
        self.assertFalse(pgconn.conn_actif)
        
    def test_conn_actif_est_False_si_parametres_incorrects(self):
        pgconn = PgConn(hote, bdd, port, 'bgfxnnfhs', 'vdsgvsgv')
        self.assertEqual(pgconn.connection, None)
        self.assertFalse(pgconn.conn_actif)
    
    def test_reussir_deconnexion_si_la_connexion_etait_etablie(self):
        pgconn = PgConn(hote, bdd, port, utilisateur, mdp)
        self.assertEqual(pgconn.connection.closed, 0) # connexion ouverte
        self.assertTrue(pgconn.conn_actif)
        pgconn.deconnection_postgres()
        self.assertNotEqual(pgconn.connection.closed, 0) # connexion fermee
        self.assertFalse(pgconn.conn_actif)
    
    def test_deconnexion_sans_impact_si_la_connexion_netait_pas_etablie(self):
        pgconn = PgConn(hote, bdd, port, utilisateur, mdp, connection_directe=False)
        self.assertEqual(pgconn.connection, None) # connexion ouverte
        self.assertFalse(pgconn.conn_actif)
        pgconn.deconnection_postgres()
        self.assertEqual(pgconn.connection, None) # connexion fermee
        self.assertFalse(pgconn.conn_actif)
        
    def test_la_requete_renvoie_moins1_si_parametres_pgconn_et_syntaxes_requete_corrects_et_requete_sans_modif_lignes(self):
        pgconn = PgConn(hote, bdd, port, utilisateur, mdp)
        nb = pgconn.execute_commit('''CREATE TABLE test (id serial, nom text);''')
        self.assertEqual(nb, -1)
        nb = pgconn.execute_commit('''DROP TABLE test;''')
        self.assertEqual(nb, -1)
            
    def test_execute_commit_renvoie_le_nombre_de_lignes_modifie_si_parametres_pgconn_et_syntaxe_requete_corrects(self):
        pgconn = PgConn(hote, bdd, port, utilisateur, mdp)
        nb = pgconn.execute_commit('''CREATE TABLE test (id serial, nom text);''')
        nb = pgconn.execute_commit('''INSERT INTO test (nom) VALUES('Nom1'), ('Nom2'), ('Nom3');''')    
        self.assertEqual(nb, 3)
        nb = pgconn.execute_commit('''SELECT * FROM test;''')
        self.assertEqual(nb, 3)  
        nb = pgconn.execute_commit('''DROP TABLE test;''')
    
    def test_execute_commit_echoue_si_erreur_requete(self):
        pgconn = PgConn(hote, bdd, port, utilisateur, mdp)
        self.assertRaises(Exception, pgconn.execute_commit, '''CREATE TABLE test ERREUR DE FRAPPEid serial, nom text);''')
    
    def test_execute_recupere_renvoie_une_liste_de_tuple_si_synthaxe_correcte(self):
        pgconn = PgConn(hote, bdd, port, utilisateur, mdp)
        nb = pgconn.execute_commit('''CREATE TABLE test (id serial, nom text);''')
        nb = pgconn.execute_commit('''INSERT INTO test (nom) VALUES('Nom1'), ('Nom2'), ('Nom3');''')
        resultat = pgconn.execute_recupere('''SELECT * FROM test;''')
        self.assertEqual(type(resultat), type(list()))
        self.assertTupleEqual(resultat[0], (1, 'Nom1'))
        self.assertTupleEqual(resultat[1], (2, 'Nom2'))
        self.assertTupleEqual(resultat[2], (3, 'Nom3'))
        nb = pgconn.execute_commit('''DROP TABLE test;''')
    
    def test_execute_recupere_echoue_si_erreur_requete(self):
        pgconn = PgConn(hote, bdd, port, utilisateur, mdp)
        self.assertRaises(Exception, pgconn.execute_recupere, '''SELECT test ERREUR DE FRAPPE FROM test;''')
        
    def test_execute_many_renvoie_le_nombre_de_lignes_modifie_si_parametres_pgconn_et_syntaxe_requete_corrects(self):
        pgconn = PgConn(hote, bdd, port, utilisateur, mdp)
        nb = pgconn.execute_commit('''CREATE TABLE test (id serial, nom text);''')
        nb = pgconn.execute_many('''INSERT INTO test (nom) VALUES(%s);''', [('Nom1',), ('Nom2',),('Nom3',)])    
        self.assertEqual(nb, 3)
        nb = pgconn.execute_many('''INSERT INTO test (nom) VALUES(%s);''', [('Nom4',), ('Nom5',)])    
        self.assertEqual(nb, 2)
        nb = pgconn.execute_commit('''DROP TABLE test;''')
    
    def test_execute_many_echoue_si_erreur_requete(self):
        pgconn = PgConn(hote, bdd, port, utilisateur, mdp)
        self.assertRaises(Exception, pgconn.execute_many, '''SELECT test ERREUR DE FRAPPE FROM test;''')
    
    
class TestPGOutils(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.system('''psql -h {0} -p {1} -U {2} -c "CREATE DATABASE {3};"'''.format(hote, port, utilisateur, bdd))
    
    @classmethod
    def tearDownClass(cls):
        os.system('''psql -h {0} -p {1} -U {2} -c "DROP DATABASE {3};"'''.format(hote, port, utilisateur, bdd))


    def setUp(self):
        self.pgoutils = PgOutils(hote, bdd, port, utilisateur, mdp)

    def tearDown(self):
        self.pgoutils.deconnecter()
        
    def test_execution_renvoie_False_moins1_en_cas_echec_requete(self):
        reussite, nb = self.pgoutils.execution('''CREATE TABLE ERREUR DE FRAPPE test(id serial);''')
        self.assertFalse(reussite)
        self.assertEqual(nb, -1)
    
    def test_execution_renvoie_True_en_cas_reussite_requete(self):
        reussite, nb = self.pgoutils.execution('''CREATE TABLE test(id serial);''')
        self.assertTrue(reussite)
        self.assertEqual(nb, -1)
        reussite, nb = self.pgoutils.execution('''DROP TABLE test;''')
        self.assertTrue(reussite)
        self.assertEqual(nb, -1)      

    def test_lister_schemas_renvoie_la_liste_des_schemas(self):
        schemas = self.pgoutils.lister_schemas()
        self.assertEqual(schemas,['pg_toast', 'pg_temp_1', 'pg_toast_temp_1', 'pg_catalog', 'public', 'information_schema'])
    
    def test_lister_schemas_commencant_par_renvoie_la_liste_des_schemas_correspondante(self):
        schemas = self.pgoutils.lister_schemas_commencant_par('pg')
        self.assertEqual(schemas,['pg_toast', 'pg_temp_1', 'pg_toast_temp_1', 'pg_catalog'])
        schemas = self.pgoutils.lister_schemas_commencant_par('pub')
        self.assertEqual(schemas,['public'])
    
    def test_lister_schemas_commencant_par_prefixe_inexistant_renvoie_une_liste_vide(self):
        schemas = self.pgoutils.lister_schemas_commencant_par('blabla')
        self.assertEqual(schemas,[])
        
    def test_creer_schema_et_effacer_schema_fonctionne_et_renvoie_True(self):
        schema = 'schema_test'
        schemas = self.pgoutils.lister_schemas()
        self.assertTrue(schema not in schemas)
        reussite, nb = self.pgoutils.creer_schema(schema)
        schemas = self.pgoutils.lister_schemas()
        self.assertTrue(schema in schemas)
        self.assertTrue(reussite)
        reussite, nb = self.pgoutils.effacer_schema(schema)
        schemas = self.pgoutils.lister_schemas()
        self.assertTrue(schema not in schemas)
        self.assertTrue(reussite)
    
    def test_creer_schema_si_inexistant_fonctionne_et_renvoie_True(self):
        schema = 'schema_test'
        schemas = self.pgoutils.lister_schemas()
        self.assertTrue(schema not in schemas)
        reussite, nb = self.pgoutils.creer_schema_si_inexistant(schema)
        schemas = self.pgoutils.lister_schemas()
        self.assertTrue(schema in schemas)
        self.assertTrue(reussite)
        self.pgoutils.effacer_schema('schema_test')
        
    def test_creer_schema_deja_existant_renvoie_False_par_methode_creer_schema(self):
        reussite, nb = self.pgoutils.creer_schema('public')
        self.assertFalse(reussite)
        self.assertEqual(nb, -1)
    
    def test_creer_schema_deja_existant_renvoie_True_par_methode_creer_schema_si_inexistant(self):
        reussite, nb = self.pgoutils.creer_schema_si_inexistant('public')
        self.assertTrue(reussite)
        self.assertEqual(nb, -1)
    
    def test_effacer_schema_inexistant_renvoie_True_et_moins1(self):
        schema = 'schema_inexistant'
        reussite, nb = self.pgoutils.effacer_schema(schema)
        self.assertTrue(reussite)
        self.assertEqual(-1, nb)
        
    def test_effacer_schemas_fonctionne_et_renvoie_True_et_nb_schemas(self):
        schemas = ['test1', 'test2']
        for schema in schemas:
            self.pgoutils.creer_schema_si_inexistant(schema)
        reussite, nb = self.pgoutils.effacer_schemas(schemas)
        self.assertTrue(reussite)
        self.assertEqual(nb, 2)
    
    def test_effacer_schemas_avec_liste_vide_et_renvoie_True_et_0(self):
        schemas = []
        reussite, nb = self.pgoutils.effacer_schemas(schemas)
        self.assertTrue(reussite)
        self.assertEqual(nb, 0)
        
        
        
    
        


if __name__ == "__main__":
    unittest.main()