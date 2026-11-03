# Rapport de synthèse ETL et audit de qualité des données

**Projet M2 DataScale** : consommation électrique et population de Paris et d'Évry-Courcouronnes.

Ce projet alimente trois tables cibles à partir de six sources, mesure la qualité de ces données et définit, pour chaque mesure, le processus d'amélioration que nous allons appliquer. La cohérence n'y figure pas, car elle sera traitée sur un autre jeu de données.

Le SQL ci-dessous est une première formalisation du mapping : **les données ne sont pas encore préparées**. Les requêtes partent des tables sources telles quelles et supposent que `NB_KW_Jour` et `Salair_Moyen` sont numériques. La fonction `UNACCENT` nécessite l'extension PostgreSQL du même nom.

### Création des tables cibles

```sql
CREATE EXTENSION IF NOT EXISTS unaccent;

-- Cible 1 : Paris
CREATE TABLE Consommation_IRIS_Paris (
    ID_IRIS VARCHAR(50) PRIMARY KEY,
    Conso_moyenne_annuelle DECIMAL(12, 2)
);

-- Cible 2 : Évry
CREATE TABLE Consommation_IRIS_Evry (
    ID_IRIS VARCHAR(50) PRIMARY KEY,
    Conso_moyenne_annuelle DECIMAL(12, 2)
);

-- Cible 3 : CSP
CREATE TABLE Consommation_CSP (
    ID_CSP VARCHAR(50) PRIMARY KEY,
    Conso_moyenne_annuelle DECIMAL(12, 2),
    Salaire_Moyen DECIMAL(12, 2)
);
```

### Alimentation de `Consommation_IRIS_Paris`

À Paris, `Nom_Rue` et `ID_Rue` contiennent le même libellé. Le rapprochement se fait sur la rue et sur la ville, car `ID_Ville` contient un nom de ville et non un code postal.

```sql
INSERT INTO Consommation_IRIS_Paris (ID_IRIS, Conso_moyenne_annuelle)
SELECT
    i.ID_IRIS,
    AVG(c.NB_KW_Jour) * 365 AS Conso_moyenne_annuelle
FROM Consommation_Paris c
JOIN IRIS i
    ON LOWER(TRIM(c.Nom_Rue)) = LOWER(TRIM(i.ID_Rue))
   AND i.ID_Ville = 'Paris'
GROUP BY i.ID_IRIS;
```

### Alimentation de `Consommation_IRIS_Evry`

À Évry, `Nom_Rue` contient le type de voie (`BOULEVARD AGUADO`) et `ID_Rue` seulement le nom (`aguado`). Le rapprochement compare donc la fin du libellé.

```sql
INSERT INTO Consommation_IRIS_Evry (ID_IRIS, Conso_moyenne_annuelle)
SELECT
    i.ID_IRIS,
    AVG(c.NB_KW_Jour) * 365 AS Conso_moyenne_annuelle
FROM Consommation_Evry c
JOIN IRIS i
    ON LOWER(TRIM(c.Nom_Rue)) LIKE ('% ' || LOWER(TRIM(i.ID_Rue)))
   AND i.ID_Ville = 'Evry-Courcouronnes'
GROUP BY i.ID_IRIS;
```

### Alimentation de `Consommation_CSP`

`Adresse` étant un texte libre, elle est comparée à l'adresse reconstruite à partir de `N` et `Nom_Rue`, sans tenir compte de la casse, des accents et des espaces.

```sql
INSERT INTO Consommation_CSP (ID_CSP, Conso_moyenne_annuelle, Salaire_Moyen)
WITH Population_Consommation AS (
    -- Paris : Nom_Rue ne contient pas le type de voie
    SELECT p.CSP, c.NB_KW_Jour
    FROM Population_Paris p
    JOIN Consommation_Paris c
      ON REPLACE(LOWER(UNACCENT(p.Adresse)), ' ', '')
       = 'paris,' || c.N || 'rue' || REPLACE(LOWER(UNACCENT(c.Nom_Rue)), ' ', '')
    UNION ALL
    -- Évry : Nom_Rue contient déjà le type de voie
    SELECT p.CSP, c.NB_KW_Jour
    FROM Population_Evry p
    JOIN Consommation_Evry c
      ON REPLACE(LOWER(UNACCENT(p.Adresse)), ' ', '')
       = 'evry-courcouronnes,91000,' || c.N || REPLACE(LOWER(UNACCENT(c.Nom_Rue)), ' ', '')
)
SELECT
    csp.ID_CSP,
    AVG(pc.NB_KW_Jour) * 365 AS Conso_moyenne_annuelle,
    csp.Salair_Moyen AS Salaire_Moyen
FROM Population_Consommation pc
JOIN CSP csp ON LOWER(TRIM(pc.CSP)) = LOWER(TRIM(csp.ID_CSP))
GROUP BY csp.ID_CSP, csp.Salair_Moyen;
```

## Contraintes d'intégrité référentielle

Certains liens ne peuvent pas être de vraies clés étrangères SQL, car les valeurs ne sont pas identiques d'une table à l'autre. Ils sont garantis par les jointures ci-dessus.

**Tables sources et référentiels**

| Attribut | Lien | Type |
|---|---|---|
| `Population_Evry(CSP)` | `CSP(ID_CSP)` | Clé étrangère |
| `Population_Paris(CSP)` | `CSP(ID_CSP)` | Clé étrangère attendue, non respectée (libellés) |
| `Population_Paris(Adresse)` | `Consommation_Paris(N, Nom_Rue)` | Lien logique après normalisation |
| `Population_Evry(Adresse)` | `Consommation_Evry(N, Nom_Rue)` | Lien logique après normalisation |
| `Consommation_Paris(Nom_Rue)` | `IRIS(ID_Rue)`, avec `ID_Ville = 'Paris'` | Lien logique |
| `Consommation_Evry(Nom_Rue)` | `IRIS(ID_Rue)`, avec `ID_Ville = 'Evry-Courcouronnes'` | Lien logique sur la fin du libellé |

**Tables cibles**

| Clé primaire | Valeur issue de |
|---|---|
| `Consommation_IRIS_Paris(ID_IRIS)` | `IRIS(ID_IRIS)` |
| `Consommation_IRIS_Evry(ID_IRIS)` | `IRIS(ID_IRIS)` |
| `Consommation_CSP(ID_CSP)` | `CSP(ID_CSP)` |

## Audit de qualité

### Périmètre d'évaluation

| Facteur | Étape | Attributs ciblés |
|---|---|---|
| Conformité | Source et cible | `Code_Postal` (Paris, Évry), `CSP` (Paris, Évry, par rapport au référentiel) |
| Hétérogénéité des échelles | Source et transformation | `NB_KW_Jour` (Wh ou kWh) |
| Complétude | Source et transformation | `NB_KW_Jour`, `Adresse` et `ID_Adr` (Paris, Évry) |
| Doublons sémantiques | Source | `Nom_Rue`, `Nom` et `Prénom` (Paris, Évry) |

### Résultats

Les objectifs sont des propositions.

| Mesure | Paris | Évry | Objectif |
|---|---|---|---|
| Conformité de `Code_Postal` | 92,31 % | 75 % | 100 % |
| Conformité de `CSP` | 0 % | 100 % | 100 % des libellés reconnus |
| Valeurs de `NB_KW_Jour` au-dessus de 1 000 | 13 sur 13 | 0 sur 12 | 0 après conversion |
| Complétude de `NB_KW_Jour` | 100 % | 58,33 % | Valeurs manquantes demandées à la source |
| Complétude de `Adresse` | 100 % | 66,67 % | Valeurs manquantes demandées à la source |
| Complétude de `ID_Adr` | 100 % | 100 % | 100 % |
| Doublons dans une même table | 0 | 0 | 0 |
| Identités présentes dans les deux villes | 6 | 6 | Chaque cas examiné |

### Démarche d'amélioration commune

Chaque mesure suit le même cycle :

1. **Mesurer** le niveau de qualité avec la métrique définie.
2. **Analyser** les lignes en erreur pour distinguer une vraie erreur d'une règle mal adaptée.
3. **Corriger** dans une copie de travail, sans jamais modifier les fichiers sources.
4. **Tracer** chaque correction : table, ligne, valeur d'origine, nouvelle valeur et règle appliquée.
5. **Prévenir** le retour de l'erreur par un contrôle en entrée du pipeline.
6. **Mesurer à nouveau** et comparer le résultat à l'objectif.

Une valeur n'est corrigée automatiquement que si la règle ne laisse aucun doute. Dans les autres cas, elle est signalée pour validation par le responsable des données.

## 1. Conformité à un format, une codification

**Définition :** vérifie que les données respectent un format prédéfini ou appartiennent à un ensemble de valeurs autorisées.

**Métrique :** taux de conformité, soit le pourcentage de valeurs respectant le format attendu ou présentes dans la table de référence.

**Attributs concernés**

- `Code_Postal` dans `Consommation_Paris` et `Consommation_Evry` : exactement 5 chiffres.
- `CSP` dans `Population_Paris` et `Population_Evry` : doit correspondre à un `ID_CSP` du référentiel `CSP`.

**Constat**

- `Consommation_Paris` contient 1 code postal invalide sur 13 : `750050`.
- `Consommation_Evry` en contient 3 sur 12 : `9100000`, `91000a` et `910000`.
- Dans `Population_Paris`, la CSP est un libellé (`Cadre`, `Employé`...), alors que `CSP.ID_CSP` est un code. Le taux est donc de 0 %.
- `Population_Evry` utilise les codes attendus : 100 % de conformité.

**Comment nous allons améliorer**

1. **Codes postaux :** remplacer la valeur invalide par le code déjà utilisé pour la même rue et la même ville. Par exemple, `750050` (rue Broca) devient `75005`. Sans code de référence fiable, la ligne est signalée.
2. **CSP de Paris :** créer une table de correspondance entre libellés et codes, à faire valider :

   | Libellé dans `Population_Paris` | `ID_CSP` |
   |---|---|
   | Agriculteur | 1 |
   | Commerçant / Artisan | 2 |
   | Cadre ; cadres et professions intellectuelles supérieures | 3 |
   | Professions intermédiaires | 4 |
   | Employé | 5 |
   | Ouvrier | 6 |

3. **Libellés sans code :** `Etudiant` et `Retraitée` n'ont aucune catégorie dans le référentiel. Ils restent signalés tant qu'il n'est pas décidé de les exclure ou d'étendre le référentiel. Le taux atteindrait alors 83,33 % à Paris.
4. **Prévention :** refuser à l'entrée tout code postal qui ne compte pas 5 chiffres et toute CSP absente du référentiel.

## 2. Hétérogénéité des échelles

**Définition :** détecte les ruptures d'homogénéité dans les unités de mesure, qui faussent les calculs.

**Métrique :** taux d'anomalies d'échelle, soit le pourcentage de valeurs négatives ou supérieures à 1 000.

**Attribut concerné :** `NB_KW_Jour` dans `Consommation_Paris` et `Consommation_Evry`.

**Constat**

L'écart d'échelle entre les deux villes est d'un **facteur 1 000**, ce qui correspond à une confusion entre Wh et kWh. La médiane est de 14 000,85 à Paris contre 8,81 à Évry. Le seuil de 100 000 utilisé jusqu'ici est trop élevé pour détecter cet écart. Avec un seuil de 1 000, les 13 valeurs de Paris sont signalées, et aucune des 12 lignes d'Évry.

**Comment nous allons améliorer**

1. **Détection :** signaler toute valeur de `NB_KW_Jour` négative ou supérieure à 1 000.
2. **Harmonisation :** diviser les valeurs de Paris par 1 000 dans une colonne dédiée, en conservant la valeur d'origine.
3. **Prévention :** ajouter l'unité attendue à la description de chaque source et bloquer le chargement si des valeurs dépassent le seuil.
4. **Contrôle :** vérifier qu'aucune valeur ne dépasse plus le seuil, puis recalculer les moyennes des tables cibles. `Consommation_CSP` ne doit jamais mélanger deux unités.

## 3. Complétude des données

**Définition :** évalue la proportion de données manquantes par rapport au volume nécessaire aux calculs cibles.

**Métrique :** taux de valeurs manquantes par attribut clé, soit le nombre de valeurs nulles, vides, `nan` ou `null`, divisé par le nombre total de lignes.

**Attributs concernés**

- `NB_KW_Jour` dans `Consommation_Paris` et `Consommation_Evry` : sans cette mesure, la ligne ne peut pas contribuer à `Conso_moyenne_annuelle`.
- `Adresse` dans `Population_Paris` et `Population_Evry`, et `ID_Adr` dans `Consommation_Paris` et `Consommation_Evry` : ils identifient l'adresse de la personne et du compteur. Sans adresse, une personne ne peut pas être rattachée à une consommation pour `Consommation_CSP`.

**Constat**

- `NB_KW_Jour` manque sur 5 lignes sur 12 dans `Consommation_Evry`.
- `Adresse` manque sur 4 lignes sur 12 dans `Population_Evry`.
- Le salaire moyen de la CSP 6 (`Ouvriers / ouvrières`) est absent, alors qu'il alimente `Consommation_CSP`.

**Comment nous allons améliorer**

1. **Source :** transmettre la liste des lignes incomplètes au fournisseur des données, car c'est le seul moyen de retrouver les vraies valeurs.
2. **Consommations manquantes :** les laisser à `NULL`, sans les remplacer par 0, ce qui ferait baisser artificiellement les moyennes. Une estimation par la médiane de la rue ou de l'IRIS ne serait utilisée qu'avec validation, et serait marquée comme estimée.
3. **Adresses manquantes :** ne pas les deviner. Les personnes concernées sont exclues de la jointure et comptées à part.
4. **Transparence :** indiquer avec chaque moyenne le nombre de valeurs utilisées et le nombre de valeurs exclues.
5. **Prévention :** déclencher une alerte si le taux de complétude d'un attribut clé passe sous un seuil à fixer.

## 4. Détection et élimination des doublons sémantiques

**Définition :** identifie des enregistrements qui représentent la même entité réelle malgré des différences d'écriture (fautes de frappe, abréviations, variations de format).

**Métrique :** part des paires testées dont la similarité de Levenshtein, après normalisation, dépasse strictement 0,85.

**Attributs concernés**

- `Nom_Rue` dans `Consommation_Paris` et `Consommation_Evry`. Exemple : `Rue de la Paix`, `R. de la Paix` et `Rue de lapaix`.
- `Nom` et `Prénom` dans `Population_Paris` et `Population_Evry`. Exemple : `Dupont Jean` et `Dupond Jean`.

**Constat**

- Aucune paire ne dépasse le seuil de similarité de 0,85 à l'intérieur d'une même table.
- Six identités figurent à la fois dans `Population_Paris` et `Population_Evry` : Bernard Sophie, Dubois Karim, Dupont Julien, Lefèvre Amélie, Martin Claire et Moreau Thomas. Leurs CSP concordent une fois les libellés de Paris convertis, mais leurs adresses diffèrent.

Il peut s'agir de doublons, d'homonymes ou de personnes ayant déménagé. Le contrôle actuel ne compare pas les deux villes et ne peut donc pas les repérer.

**Comment nous allons améliorer**

1. **Comparaison :** étendre le contrôle aux paires entre les deux tables, en gardant la normalisation actuelle (casse, accents, abréviations de voies) et le seuil de 0,85.
2. **Qualification :** classer chaque paire à l'aide des attributs disponibles : similarité du nom, CSP et adresse.
3. **Décision :** faire valider chaque paire. Un même nom ne suffit pas à fusionner deux personnes.
4. **Élimination :** pour un doublon confirmé, garder un seul enregistrement de référence et conserver le lien vers les identifiants d'origine.
5. **Prévention :** rejouer ce contrôle à chaque nouveau chargement, et ajouter un identifiant de personne commun aux deux sources si c'est possible.
