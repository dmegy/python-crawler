python-crawler
===============

Petit crawler en python.

Créé initialement pour télécharger des pdfs de sujets d'examens de mathématiques en masse, et les archiver.


Pour activer l'environnement virtuel et installer les packages
---


```bash
python3 -m venv venv
source venv/bin/activate
pip install requests
pip install bs4
pip install PyPDF2
```

Que faire pour plusieurs projets ?
---

Pour plusieurs projets, recloner tout le repo.

Fonctionnalités
---


Décisions prises
---

Suppression de beaucoup de champs dans la base de données : keywords, subject etc pour les pdfs, au début il y avait même une preview du codument, des champs de commentaires etc. Pareil pour les métadonnées des pages web.

Stocker le Producer en brut, application-data en brut etc, ne pas prendre de décisions sur ce que l'on stocke, ça doit être les données brutes récupérées.

Ne pas stocker tout le response header en json, inutile, juste le code, content-length etc. Last-modified est-il utile ?



Décisions en suspens
---
- Est-ce utile d'avoir les premiers octets des fichiers ?
- Est-ce vraiment utile de stocker le checksum ?

TODO
---

- Throttling pour le téléchargement
- Contraintes pour les téléchargements à bouger dans des fichiers de config pur ne pas avoir à modifier le fichier .py ?




