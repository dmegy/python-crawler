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



Décisions en suspens
---
- Est-ce utile d'avoir les premiers octets des fichiers ?
- Est-ce vraiment utile de stocker le checksum ?
- devrait)on faire une request HEAD lors du crawl pour ne suivre que des vrais html et ne sticker que des urls de vrais documents ? ( et donc sauter l'étape verify, quelque part ?). Ca rend le crawl bcp plus long et donc ça remplit la database plus lentement, mais ça éviter l'étape de vérifictaion.

TODO
---

- repérer les domaines qui timeout et les ajouter à un fichier texte de "mauvais" domaines ?
- lancer en mémoire la liste des documents déjà ajoutés ? Car ici on pourrait ajouter à la base un document déjà présent (pas lors de la même session mais lors d'une session ultérieure)
- Throttling pour le téléchargement
- Contraintes pour les téléchargements à bouger dans des fichiers de config pour ne pas avoir à modifier le fichier .py ?
- se renseigner sur le fonctionnement d'autres crawlers.

NOT TODO
---

paralléliser
asynchrone ?
autres gadjets ?



Décisions prises
---

Suppression de beaucoup de champs dans la base de données : keywords, subject etc pour les pdfs, au début il y avait même une preview du codument, des champs de commentaires etc. Pareil pour les métadonnées des pages web.

Stocker le Producer en brut, application-data en brut etc, ne pas prendre de décisions sur ce que l'on stocke, ça doit être les données brutes récupérées.

Ne pas stocker tout le response header en json, inutile, juste le code, content-length etc. Last-modified est-il utile ?


