'''
@author: antoine.herman
'''
import unittest
import psycopg2
import os
from pg.pgbasics import *
from pg.pgio import PgExport

hote = 'localhost'
bdd = 'test_pg'
utilisateur = 'postgres'
mdp = 'postgres'
port = '5432'

class TestPGExport(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.system('''psql -h {0} -p {1} -U {2} -c "CREATE DATABASE {3};"'''.format(hote, port, utilisateur, bdd))
        pgexport = PgExport(hote, bdd, port, utilisateur, mdp)
        pgexport.execution('''CREATE TABLE public.test (id serial, nom TEXT, prenom TEXT, age INTEGER);''')
        pgexport.execution_multiple('''INSERT INTO public.test (nom, prenom, age) VALUES (%s,%s,%s);''', 
                                    [('McFly', 'Marty', 20),
                                     ('Doc', 'Emmett', 60),
                                     ('Tannen', 'Biff', 22),
                                     ('Abidbol', 'Georges', 45),
                                     ('Capone', 'Al', 50)])
    
    @classmethod
    def tearDownClass(cls):
        os.system('''psql -h {0} -p {1} -U {2} -c "DROP DATABASE {3};"'''.format(hote, port, utilisateur, bdd))
        if os.path.exists('test.sql'):
            os.remove('test.sql')
    
    def setUp(self):
        self.pgexport = PgExport(hote, bdd, port, utilisateur, mdp)
            
    def tearDown(self):
        if os.path.exists('export_test.csv'):
            os.remove('export_test.csv')
            
    def test_export_table_en_csv(self):
        self.pgexport.exporter_table_vers_csv('public', 'test', 'export_test.csv')
        self.assertTrue(os.path.exists('export_test.csv'))
        with open('export_test.csv', encoding='utf-8') as f:
            lignes = f.readlines()
        self.assertEqual(lignes[0], 'id|nom|prenom|age\n')
        self.assertEqual(lignes[1], '1|McFly|Marty|20\n')
        self.assertEqual(lignes[3], '3|Tannen|Biff|22\n')

    def test_export_table_en_csv_avec_limit(self):
        self.pgexport.exporter_table_vers_csv('public', 'test', 'export_test.csv', limit = 2)
        self.assertTrue(os.path.exists('export_test.csv'))
        with open('export_test.csv', encoding='utf-8') as f:
            lignes = f.readlines()
        self.assertEqual(lignes[0], 'id|nom|prenom|age\n')
        self.assertEqual(lignes[1], '1|McFly|Marty|20\n')
        self.assertEqual(lignes[2], '2|Doc|Emmett|60\n')
        self.assertEqual(len(lignes), 3)
    
    def test_export_requete_en_csv(self):
        self.pgexport.exporter_requete_vers_csv('SELECT id, prenom, age FROM public.test;', 'export_test.csv')
        self.assertTrue(os.path.exists('export_test.csv'))
        with open('export_test.csv', encoding='utf-8') as f:
            lignes = f.readlines()
        self.assertEqual(lignes[0], 'id|prenom|age\n')
        self.assertEqual(lignes[1], '1|Marty|20\n')
        self.assertEqual(lignes[4], '4|Georges|45\n')
        
if __name__ == '__main__':
    unittest.main()