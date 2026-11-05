"""Tableau de bord des metriques de qualite produites par les DAG Airflow.

Lancement, depuis la racine du projet : streamlit run dashboard/app.py
Le tableau de bord lit les CSV d'audit tels quels : aucun chiffre n'est ecrit ici.
"""
from datetime import datetime
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

RACINE = Path(__file__).resolve().parent.parent
RESULTATS = RACINE / "dags" / "data" / "output" / "audit_results"
AUDITS = {
    "conformite": ("audit_conformite.csv", "mesure_conformite"),
    "completude": ("audit_completude.csv", "metrique_completude"),
    "heterogeneite": ("audit_heterogeneite.csv", "mesure_heterogeneite_granularite"),
    "doublons": ("audit_doublons.csv", "metrique_doublons_semantiques"),
}

LIBELLES_MESURES = {
    "Conformite_Code_Postal": "Code postal",
    "Conformite_Referentiel_CSP_Brute": "CSP brute",
    "Conformite_Referentiel_CSP_Normalisee": "CSP après correspondance",
    "Echelle_Conversion_Numerique": "Conversion en nombre",
    "Echelle_Valeurs_Atypiques_IQR": "Valeurs non atypiques (IQR)",
    "Granularite_Rues_Multi_IRIS": "Rues à un seul IRIS",
    "Granularite_Consommations_IRIS_Ambigu": "Compteurs à IRIS non ambigu",
}
LIBELLES_TABLES = {
    "Consommation_Paris": "Paris",
    "Consommation_Evry": "Évry",
    "Population_Paris": "Paris",
    "Population_Evry": "Évry",
    "IRIS": "Référentiel",
}

ETAT_ATTEINT = "100 %"
ETAT_SOUS = "Moins de 100 %"
COULEURS_ETATS = {ETAT_ATTEINT: "#0ca30c", ETAT_SOUS: "#ec835a"}
ENCRE = "#0b0b0b"
ENCRE_SECONDAIRE = "#52514e"
GRILLE = "#e1e0d9"
AXE = "#c3c2b7"


def trouver_audit(nom_fichier, dossier):
    """Priorite au CSV produit par le DAG, sinon a une copie a la racine du projet."""
    for chemin in (RESULTATS / dossier / nom_fichier, RACINE / nom_fichier):
        if chemin.is_file():
            return chemin
    return None


def charger_audits():
    donnees, sources, absents = {}, [], []
    for cle, (nom_fichier, dag) in AUDITS.items():
        chemin = trouver_audit(nom_fichier, cle)
        if chemin is None:
            absents.append((nom_fichier, dag))
            continue
        donnees[cle] = pd.read_csv(chemin, encoding="utf-8-sig")
        sources.append((nom_fichier, chemin, datetime.fromtimestamp(chemin.stat().st_mtime)))
    return donnees, sources, absents


def nombre(valeur, decimales=2):
    """Format francais : 1 590 ; 58,33 ; 100."""
    # float() : Seuil_Alerte melange du texte et des nombres, pandas le lit donc en texte.
    texte = f"{float(valeur):,.{decimales}f}".replace(",", " ").replace(".", ",")
    return texte.rstrip("0").rstrip(",") if "," in texte else texte


def pct(valeur):
    return f"{nombre(valeur)} %"


def table_lisible(table):
    return LIBELLES_TABLES.get(table, table)


def pluriel(nb, mot):
    return f"{nb} {mot}{'s' if nb > 1 else ''}"


def graphique_taux(lignes):
    """Barres horizontales de 0 a 100 %, une par mesure, valeur affichee au bout."""
    df = lignes.copy()
    df["etat"] = df["taux"].map(lambda t: ETAT_ATTEINT if t >= 100 else ETAT_SOUS)
    df["valeur"] = df["taux"].map(pct)
    y = alt.Y("mesure:N", sort=None, title=None,
              axis=alt.Axis(labelLimit=260, labelColor=ENCRE_SECONDAIRE, labelFontSize=13,
                            ticks=False, domainColor=AXE))
    infobulle = [alt.Tooltip("mesure:N", title="Mesure"), alt.Tooltip("valeur:N", title="Taux"),
                 alt.Tooltip("detail:N", title="Détail")]
    barres = alt.Chart(df).mark_bar(size=18, cornerRadiusEnd=4).encode(
        x=alt.X("taux:Q", title=None, scale=alt.Scale(domain=[0, 100], nice=False),
                axis=alt.Axis(values=[0, 25, 50, 75, 100], labelColor=ENCRE_SECONDAIRE,
                              gridColor=GRILLE, domainColor=AXE, tickColor=AXE,
                              labelExpr="datum.value + ' %'")),
        y=y,
        color=alt.Color("etat:N", title=None,
                        scale=alt.Scale(domain=list(COULEURS_ETATS), range=list(COULEURS_ETATS.values())),
                        legend=alt.Legend(orient="bottom", labelColor=ENCRE_SECONDAIRE, labelFontSize=12)),
        tooltip=infobulle,
    )
    valeurs = alt.Chart(df).mark_text(align="left", dx=6, color=ENCRE, fontSize=13).encode(
        x="taux:Q", y=y, text="valeur:N", tooltip=infobulle,
    )
    # La marge de droite garde la place des valeurs, a toutes les largeurs d'ecran.
    return (barres + valeurs).properties(
        height=len(df) * 36 + 10, padding={"left": 5, "top": 5, "bottom": 5, "right": 62},
    ).configure_view(stroke=None)


def lignes_conformite(df):
    return pd.DataFrame({
        "mesure": [f"{LIBELLES_MESURES.get(m, m)} · {table_lisible(t)}"
                   for m, t in zip(df["Nom_Mesure"], df["Table_Testee"])],
        "taux": df["Taux_Conformite_Pct"],
        "detail": [f"{pluriel(int(e), 'ligne')} en écart sur {int(n)}"
                   for e, n in zip(df["Nb_Erreurs"], df["Nb_Lignes_Total"])],
    })


def lignes_completude(df):
    return pd.DataFrame({
        "mesure": [f"{a} · {table_lisible(t)}"
                   for a, t in zip(df["attribut_audite"], df["table_auditee"])],
        "taux": df["taux_completude_pct"],
        "detail": [f"{nombre(m)} % de valeurs manquantes" for m in df["taux_valeurs_manquantes_pct"]],
    })


def tuile(colonne, titre, valeur, ok, message):
    with colonne.container(border=True):
        st.metric(titre, valeur)
        st.markdown(f"{':material/check_circle:' if ok else ':material/warning:'} {message}")


def tuile_taux(colonne, titre, lignes):
    sous = int((lignes["taux"] < 100).sum())
    tuile(colonne, f"{titre}, taux le plus bas", pct(lignes["taux"].min()), sous == 0,
          f"{pluriel(sous, 'mesure')} sur {len(lignes)} sous 100 %")


def ligne_ratio(df):
    """Ligne du rapport des medianes, absente si une mediane n'est pas calculable."""
    lignes = df[df["Nom_Mesure"].str.contains("Ratio", na=False)]
    return lignes.iloc[0] if len(lignes) else None


def afficher():
    st.set_page_config(page_title="Qualité des données", layout="wide")
    st.title("Qualité des données")
    st.write("Consommation électrique et population de Paris et d'Évry-Courcouronnes. "
             "Mesures sur les **données brutes**, avant les corrections prévues dans le README.")

    audits, sources, absents = charger_audits()
    for nom_fichier, dag in absents:
        st.warning(f"`{nom_fichier}` est introuvable. Lancez le DAG `{dag}` pour le produire.")
    if not audits:
        st.stop()

    conformite = lignes_conformite(audits["conformite"]) if "conformite" in audits else None
    completude = lignes_completude(audits["completude"]) if "completude" in audits else None
    heterogeneite = audits.get("heterogeneite")
    doublons = audits.get("doublons")
    ratio = ligne_ratio(heterogeneite) if heterogeneite is not None else None

    tuiles = st.columns(4)
    if conformite is not None:
        tuile_taux(tuiles[0], "Conformité", conformite)
    if completude is not None:
        tuile_taux(tuiles[1], "Complétude", completude)
    if ratio is not None:
        alerte = int(ratio["Nb_Erreurs"]) > 0
        tuile(tuiles[2], "Rapport d'échelle Paris / Évry", nombre(ratio["Valeur_Observee"], 0), not alerte,
              f"{'Alerte : au-dessus' if alerte else 'En dessous'} du seuil de {nombre(ratio['Seuil_Alerte'], 0)}")
    if doublons is not None:
        nb = int(doublons["nb_doublons_detectes"].sum())
        tuile(tuiles[3], "Doublons sémantiques", nombre(nb, 0), nb == 0,
              f"Sur {nombre(doublons['nb_paires_testees'].sum(), 0)} paires testées")

    gauche, droite = st.columns(2)
    if conformite is not None:
        with gauche:
            st.subheader("Conformité")
            st.altair_chart(graphique_taux(conformite), theme=None, width="stretch")
    if completude is not None:
        with droite:
            st.subheader("Complétude")
            st.altair_chart(graphique_taux(completude), theme=None, width="stretch")

    gauche, droite = st.columns(2)
    if heterogeneite is not None:
        with gauche:
            st.subheader("Hétérogénéité et granularité")
            taux = heterogeneite[~heterogeneite["Nom_Mesure"].str.contains("Ratio", na=False)]
            st.altair_chart(graphique_taux(lignes_conformite(taux)), theme=None, width="stretch")
            st.caption("Le rapport des médianes figure dans la tuile du haut : "
                       "un pourcentage n'aurait pas de sens pour cette mesure.")
    if doublons is not None:
        with droite:
            st.subheader("Doublons sémantiques")
            vue = pd.DataFrame({
                "Table": doublons["table_auditee"],
                "Attributs": doublons["attributs_audites"],
                "Comparés si même": doublons["cle_regroupement"],
                "Paires testées": doublons["nb_paires_testees"],
                "Doublons": [str(d) if p else "Aucune paire à comparer" for d, p in
                             zip(doublons["nb_doublons_detectes"], doublons["nb_paires_testees"])],
            })
            st.dataframe(vue, hide_index=True, width="stretch")
            st.caption(f"Seuil de similarité : {nombre(doublons['seuil_similarite'].iloc[0])}. "
                       "Les paires entre Paris et Évry ne sont pas comparées.")

    with st.expander("Données détaillées"):
        for cle, df in audits.items():
            st.markdown(f"**{AUDITS[cle][0]}**")
            st.dataframe(df, hide_index=True, width="stretch")

    st.caption("Sources : " + " · ".join(
        f"`{chemin.relative_to(RACINE).as_posix()}` ({date:%d/%m/%Y %H:%M})" for _, chemin, date in sources))


afficher()
