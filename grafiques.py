import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Dades inicials
data = [
    {"Recurs": "Lambda-Crypto-Bronze", "Tipus": "Lambda", "Capa": "Bronze", "Temps_segons": 55.836},
    {"Recurs": "Lambda-Posts-Bronze", "Tipus": "Lambda", "Capa": "Bronze", "Temps_segons": 180.0},
    {"Recurs": "Lambda-Crypto-Silver", "Tipus": "Lambda", "Capa": "Silver", "Temps_segons": 11.189},
    {"Recurs": "Lambda-Posts-Silver", "Tipus": "Lambda", "Capa": "Silver", "Temps_segons": 3.180},
    {"Recurs": "ETL-Job-Posts-Silver-Gold", "Tipus": "ETL", "Capa": "Gold", "Temps_segons": 191.0},
    {"Recurs": "ETL-Job-Crypto-Silver-Gold", "Tipus": "ETL", "Capa": "Gold", "Temps_segons": 125.0},
    {"Recurs": "ETL-Job-Relationships-Silver-Gold", "Tipus": "ETL", "Capa": "Gold", "Temps_segons": 104.0},
    {"Recurs": "Crawler-Bronze", "Tipus": "Crawler", "Capa": "Bronze", "Temps_segons": 110.0},
    {"Recurs": "Crawler-Silver", "Tipus": "Crawler", "Capa": "Silver", "Temps_segons": 120.0},
    {"Recurs": "Crawler-Gold", "Tipus": "Crawler", "Capa": "Gold", "Temps_segons": 107.0}
]

df = pd.DataFrame(data)

# Guardar CSV
df.to_csv("pipeline_temps_net_gold.csv", index=False)

# Ordenar
df_sorted = df.sort_values(by=["Tipus", "Capa", "Temps_segons"])

# 1. Gràfica de barres verticals 
plt.figure(figsize=(12, 6))
sns.barplot(data=df_sorted, x="Recurs", y="Temps_segons", hue="Tipus", dodge=False)
plt.xticks(rotation=45, ha="right")
plt.title("Temps per Recurs")
plt.ylabel("Temps (Segons)")
plt.xlabel("Recurs")
plt.tight_layout()
plt.savefig("grafica_1_recursos_gold.png")
plt.close()

# 2. Gràfica de donut
capa_totals = df.groupby("Capa")["Temps_segons"].sum()
color_map = {'Bronze': '#cd7f32', 'Silver': '#c0c0c0', 'Gold': '#ffd700'}
colors = [color_map[c] for c in capa_totals.index]

plt.figure(figsize=(8, 8))
plt.pie(capa_totals, labels=capa_totals.index, autopct='%1.1f%%', startangle=140, 
        colors=colors, wedgeprops=dict(width=0.4),
        pctdistance=0.80,
        textprops={'fontsize': 12})

plt.title("Percentatge de Temps Total per Capa")
#bbox_inches="tight"
plt.savefig("grafica_2_donut_gold.png", bbox_inches="tight") 
plt.close()

# 3. Gràfica de barres acumulades per capa
plt.figure(figsize=(8, 6))
sns.barplot(x=capa_totals.index, y=capa_totals.values, order=["Bronze", "Silver", "Gold"], palette=['#cd7f32', '#c0c0c0', '#ffd700'])
plt.title("Temps Total Acumulat per Capa")
plt.ylabel("Temps Total (Segons)")
plt.xlabel("Capa")
plt.tight_layout()
plt.savefig("grafica_3_acumulat_gold.png")
plt.close()