import unittest
import psycopg2
import os
from pg.pgbasics import *
from pg.pgio import PgSave, PgLoad

hote = 'localhost'
bdd = 'test_pg'
utilisateur = 'postgres'
mdp = 'postgres'
port = '5432'

demande_mdp = False

class TestPGLoad(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.system('''psql -h {0} -p {1} -U {2} -c "CREATE DATABASE {3};"'''.format(hote, port, utilisateur, bdd))
        pgoutils = PgOutils(hote, bdd, port, utilisateur, mdp)
        pgoutils.execution('''CREATE TABLE public.test (id serial, nom TEXT, prenom TEXT, age INTEGER);''')
        pgoutils.execution_multiple('''INSERT INTO public.test (nom, prenom, age) VALUES (%s,%s,%s);''', 
                                    [('McFly', 'Marty', 20),
                                     ('Doc', 'Emmett', 60),
                                     ('Tannen', 'Biff', 22),
                                     ('Abidbol', 'Georges', 45),
                                     ('Capone', 'Al', 50)])
        pgoutils.deconnecter()
        pgsave = PgSave(hote, bdd, utilisateur, mdp, demande_mdp)
        pgsave.sauvegarder_schemas_format_sql('test.sql', ['public'])
        os.system('''psql -h {0} -p {1} -U {2} -c "DROP DATABASE {3};"'''.format(hote, port, utilisateur, bdd))
    
    @classmethod
    def tearDownClass(cls):
        if(os.path.exists('test.sql')):
            os.remove('test.sql')
    
    def test_charger_fichier_dans_nvelle_base(self):
        pgload = PgLoad(hote, bdd, utilisateur, mdp, demande_mdp)
        pgload.charger_fichier_sql_dans_nouvelle_base('test.sql')
        pgoutils = PgOutils(hote, bdd, port, utilisateur, mdp)
        self.assertIn('test', pgoutils.lister_tables('public'))
        os.system('''psql -h {0} -p {1} -U {2} -c "DROP DATABASE {3};"'''.format(hote, port, utilisateur, bdd))
    


if __name__ == '__main__':
    unittest.main()