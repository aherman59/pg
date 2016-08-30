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

class TestPGExport(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.system('''psql -h {0} -p {1} -U {2} -c "CREATE DATABASE {3};"'''.format(hote, port, utilisateur, bdd))
    
    @classmethod
    def tearDownClass(cls):
        os.system('''psql -h {0} -p {1} -U {2} -c "DROP DATABASE {3};"'''.format(hote, port, utilisateur, bdd))
        if os.path.exists('test.sql'):
            os.remove('test.sql')
    
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
        pass
    
    def test_sauvegarde_table_sql(self):
        pgsave = PgSave(hote, bdd, utilisateur, mdp)
        pgsave.sauvegarder_tables_format_sql('test.sql', ['public.test'])
    
if __name__ == '__main__':
    unittest.main()