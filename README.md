# pg

License : GNU GPL v3

## Utilisation de la classe PgOutils

Création d'une classe qui hérite de PgOutils :

```python
class MonRequeteur(PgOutils):

   def __init__(self, hote=None, base=None, port=None, utilisateur=None, motdepasse=None):
        super().__init__(hote, base, port, utilisateur, motdepasse)
   
   @select_sql
   def ma_requete(self, valeur_champ):
   		pass
   
   @select_sql_avec_modification_args
   def ma_requete_avec_modif(self, valeur_champ):
   		return str(2*valeur_champ)

```

Création du fichier monrequeteur.sql correspondant qui doit être placé dans le même répertoire:

```
## MA_REQUETE
SELECT * FROM schema.table WHERE champ = {0};

## MA_REQUETE_AVEC_MODIF
SELECT * FROM schema.table WHERE champ = {0};
```  