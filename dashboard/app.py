import streamlit as st
import psycopg2
import pandas as pd
import time

# Configuration de la page
st.set_page_config(
    page_title="🔐 Détection d'Anomalies",
    page_icon="🔐",
    layout="wide"
)

st.title("🔐 Tableau de Bord - Détection d'Anomalies en Temps Réel")

# Connexion PostgreSQL
def get_connection():
    return psycopg2.connect(
        host="postgres",
        database="secudb",
        user="admin",
        password="secret"
    )

# Récupérer les données
def get_data():
    try:
        conn = get_connection()
        
        # Total événements
        total = pd.read_sql(
            "SELECT COUNT(*) as total FROM security_events",
            conn
        )
        
        # Total attaques
        attacks = pd.read_sql(
            "SELECT COUNT(*) as attacks FROM security_events WHERE is_attack = true",
            conn
        )
        
        # Derniers événements
        recent = pd.read_sql(
            """SELECT protocol_type, service, flag, 
               attack_type, is_attack, timestamp 
               FROM security_events 
               ORDER BY timestamp DESC LIMIT 20""",
            conn
        )
        
        # Attaques par type
        by_type = pd.read_sql(
            """SELECT attack_type, COUNT(*) as nombre 
               FROM security_events 
               WHERE is_attack = true
               GROUP BY attack_type 
               ORDER BY nombre DESC""",
            conn
        )
        
        conn.close()
        return total, attacks, recent, by_type
    
    except Exception as e:
        st.error(f"Erreur de connexion : {e}")
        return None, None, None, None

# Rafraîchissement automatique
placeholder = st.empty()

while True:
    total, attacks, recent, by_type = get_data()
    
    with placeholder.container():
        
        if total is not None:
            # ---- MÉTRIQUES EN HAUT ----
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric(
                    "📦 Total Événements",
                    int(total['total'][0])
                )
            
            with col2:
                st.metric(
                    "🚨 Total Attaques",
                    int(attacks['attacks'][0])
                )
            
            with col3:
                if int(total['total'][0]) > 0:
                    pct = round(
                        int(attacks['attacks'][0]) / 
                        int(total['total'][0]) * 100, 1
                    )
                    st.metric("⚠️ Taux d'Attaques", f"{pct}%")
            
            st.divider()
            
            # ---- GRAPHIQUE PAR TYPE ----
            if by_type is not None and len(by_type) > 0:
                st.subheader("📊 Attaques par Type")
                st.bar_chart(
                    by_type.set_index('attack_type')['nombre']
                )
            
            st.divider()
            
            # ---- DERNIERS ÉVÉNEMENTS ----
            st.subheader("📋 Derniers Événements")
            if recent is not None and len(recent) > 0:
                st.dataframe(
                    recent,
                    use_container_width=True
                )
        else:
            st.warning("⏳ En attente de données...")
    
    # Rafraîchir toutes les 5 secondes
    time.sleep(5)
    st.rerun()
