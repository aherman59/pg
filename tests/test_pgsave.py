import unittest
import psycopg2
import os
from pg.pgbasics import *
from pg.pgio import PgSave

hote = 'localhost'
bdd = 'test_pg'
utilisateur = 'postgres'
mdp = 'postgres'
port = '5432'

demande_mdp = True

class TestPGSave(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.system('''psql -h {0} -p {1} -U {2} -c "CREATE DATABASE {3};"'''.format(hote, port, utilisateur, bdd))
    
    @classmethod
    def tearDownClass(cls):
        os.system('''psql -h {0} -p {1} -U {2} -c "DROP DATABASE {3};"'''.format(hote, port, utilisateur, bdd))
    
    def setUp(self):
        self.pgoutils = PgOutils(hote, bdd, port, utilisateur, mdp)
        self.pgoutils.execution('''CREATE TABLE public.test (id serial, nom TEXT, prenom TEXT, age INTEGER);''')
        self.pgoutils.execution_multiple('''INSERT INTO public.test (nom, prenom, age) VALUES (%s,%s,%s);''', 
                                    [('McFly', 'Marty', 20),
                                     ('Doc', 'Emmett', 60),
                                     ('Tannen', 'Biff', 22),
                                     ('Abidbol', 'Georges', 45),
                                     ('Capone', 'Al', 50)])

    def tearDown(self):
        self.pgoutils.execution('''DROP TABLE public.test CASCADE;''')
        self.pgoutils.deconnecter()
        if os.path.exists('test.sql'):
            os.remove('test.sql')
        if os.path.exists('test.dump'):
            os.remove('test.dump')
    
    def test_commande_pg_dump_accessible(self):
        pgsave = PgSave(hote, bdd, utilisateur, mdp)
        self.assertTrue(pgsave.verification_commande_pg_dump())
    
    def test_sauvegarde_base_fonctionne_avec_parametrage_correct(self):
        self.assertFalse(os.path.exists('test.sql'))
        pgsave = PgSave(hote, bdd, utilisateur, mdp, demande_mdp)
        pgsave.sauvegarder_base_format_sql('test.sql')
        self.assertTrue(os.path.exists('test.sql'))
        with open('test.sql', encoding='utf-8') as f:
            txt = f.read()
        self.assertIn('CREATE DATABASE', txt)
        self.assertIn('CREATE TABLE test', txt)
    
    def test_sauvegarde_base_echoue_si_mdp_incorrect_et_demande_mdp_est_Vrai_reussit_sinon(self):
        if demande_mdp:
            self.assertFalse(os.path.exists('test.sql'))
            pgsave = PgSave(hote, bdd, utilisateur, 'blabla')
            p = pgsave.sauvegarder_base_format_sql('test.sql')
            self.assertFalse(os.path.exists('test.sql'))
            self.assertIn('password authentication failed', p.stderr)
        else:
            self.assertFalse(os.path.exists('test.sql'))
            pgsave = PgSave(hote, bdd, utilisateur, 'blabla', demande_mdp)
            p = pgsave.sauvegarder_base_format_sql('test.sql')
            self.assertTrue(os.path.exists('test.sql'))
            with open('test.sql', encoding='utf-8') as f:
                txt = f.read()
            self.assertIn('CREATE DATABASE', txt)
            self.assertIn('CREATE TABLE test', txt)
    
    def test_sauvegarde_table_sql_fonctionne_avec_parametrage_correct(self):
        self.assertFalse(os.path.exists('test.sql'))
        pgsave = PgSave(hote, bdd, utilisateur, mdp, demande_mdp)
        pgsave.sauvegarder_tables_format_sql('test.sql', ['public.test'])
        self.assertTrue(os.path.exists('test.sql'))
        with open('test.sql', encoding='utf-8') as f:
            txt = f.read()
        self.assertIn('CREATE TABLE test', txt)
    
    def test_sauvegarde_table_sql_echoue_si_mdp_incorrect_et_demande_mdp_est_True_reussit_sinon(self):
        if demande_mdp:
            self.assertFalse(os.path.exists('test.sql'))
            pgsave = PgSave(hote, bdd, utilisateur, 'blabla')
            p = pgsave.sauvegarder_tables_format_sql('test.sql', ['public.test'])
            self.assertFalse(os.path.exists('test.sql'))
            self.assertIn('password authentication failed', p.stderr)
        else:
            self.assertFalse(os.path.exists('test.sql'))
            pgsave = PgSave(hote, bdd, utilisateur, 'blabla', demande_mdp)
            p = pgsave.sauvegarder_tables_format_sql('test.sql', ['public.test'])
            self.assertTrue(os.path.exists('test.sql'))
            with open('test.sql', encoding='utf-8') as f:
                txt = f.read()
            self.assertIn('CREATE TABLE test', txt)
    
    def test_sauvegarde_table_dump_fonctionne_avec_parametrage_correct(self):
        self.assertFalse(os.path.exists('test.sql'))
        pgsave = PgSave(hote, bdd, utilisateur, mdp, demande_mdp)
        pgsave.sauvegarder_tables_format_dump('test.dump', ['public.test'])
        self.assertTrue(os.path.exists('test.dump'))
    
    def test_sauvegarde_table_dump_echoue_si_mdp_incorrect_et_demande_mdp_est_True_reussit_sinon(self):
        if demande_mdp:
            pgsave = PgSave(hote, bdd, utilisateur, 'blabbla')
            p = pgsave.sauvegarder_tables_format_dump('test.dump', ['public.test'])
            self.assertIn('password authentication failed', p.stderr)
        else:
            self.assertFalse(os.path.exists('test.dump'))
            pgsave = PgSave(hote, bdd, utilisateur, 'blabbla', demande_mdp)
            p = pgsave.sauvegarder_tables_format_dump('test.dump', ['public.test'])
            self.assertTrue(os.path.exists('test.dump'))
    
    def test_sauvegarde_table_dump_echoue_si_utilisateur_incorrect(self):
        pgsave = PgSave(hote, bdd, 'blabla', mdp, demande_mdp)
        p = pgsave.sauvegarder_tables_format_dump('test.dump', ['public.test'])
        if demande_mdp:
            self.assertIn('password authentication failed', p.stderr)
        else:
            self.assertIn('FATAL', p.stderr)
           
    def test_sauvegarde_schema_sql_fonctionne_avec_parametrage_correct(self):
        self.assertFalse(os.path.exists('test.sql'))
        pgsave = PgSave(hote, bdd, utilisateur, mdp, demande_mdp)
        pgsave.sauvegarder_schemas_format_sql('test.sql', ['public'])
        self.assertTrue(os.path.exists('test.sql'))
        with open('test.sql', encoding='utf-8') as f:
            txt = f.read()
        self.assertIn('CREATE TABLE test', txt)
    
    def test_sauvegarde_schema_sql_echoue_si_mdp_incorrect_et_demande_mdp_est_True_reussit_sinon(self):
        if demande_mdp:
            self.assertFalse(os.path.exists('test.sql'))
            pgsave = PgSave(hote, bdd, utilisateur, 'blabla')
            p = pgsave.sauvegarder_schemas_format_sql('test.sql', ['public'])
            self.assertFalse(os.path.exists('test.sql'))
            self.assertIn('password authentication failed', p.stderr)
        else:
            self.assertFalse(os.path.exists('test.sql'))
            pgsave = PgSave(hote, bdd, utilisateur, 'blabla', demande_mdp)
            p = pgsave.sauvegarder_tables_format_sql('test.sql', ['public'])
            self.assertTrue(os.path.exists('test.sql'))
            with open('test.sql', encoding='utf-8') as f:
                txt = f.read()
            self.assertIn('CREATE TABLE test', txt)
    
    def test_sauvegarde_schema_dump_fonctionne_avec_parametrage_correct(self):
        pgsave = PgSave(hote, bdd, utilisateur, mdp, demande_mdp)
        pgsave.sauvegarder_schemas_format_dump('test.dump', ['public'])
        self.assertTrue(os.path.exists('test.dump'))
    
    def test_sauvegarde_schema_dump_echoue_si_mdp_incorrect_et_demande_mdp_est_True_reussit_sinon(self):
        if demande_mdp:
            pgsave = PgSave(hote, bdd, utilisateur, 'blabbla')
            p = pgsave.sauvegarder_schemas_format_dump('test.dump', ['public'])
            self.assertIn('password authentication failed', p.stderr)
        else:
            self.assertFalse(os.path.exists('test.dump'))
            pgsave = PgSave(hote, bdd, utilisateur, 'blabbla', demande_mdp)
            p = pgsave.sauvegarder_schemas_format_dump('test.dump', ['public'])
            self.assertTrue(os.path.exists('test.dump'))
    
    def test_sauvegarde_schema_dump_echoue_si_utilisateur_incorrect(self):
        pgsave = PgSave(hote, bdd, 'blabla', mdp, demande_mdp)
        p = pgsave.sauvegarder_schemas_format_dump('test.dump', ['public'])
        if demande_mdp:
            self.assertIn('password authentication failed', p.stderr)
        else:
            self.assertIn('FATAL', p.stderr)
            
    
if __name__ == '__main__':
    unittest.main()