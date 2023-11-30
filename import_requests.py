import urllib.request as request
import os
from datetime import datetime

#import des packages de 2018 a 2023
# Dossier local pour enregistrer les fichiers téléchargés
yellowT = r"C:\ecole I1\ARCHITECTURE DECISIONNELLE\ATL-Datamart-main\ATL-Datamart-main\data\raw"

# Assurez-vous que le dossier local existe
if not os.path.exists(yellowT):
    os.mkdir(yellowT)

# Générez les liens de téléchargement pour chaque année et mois de 2018-01 à 2023-12
for year in range(2023, 2024):
    for month in range(1, 9):
        # Formattez l'année et le mois pour correspondre au modèle de lien
        year_month = f"{year}-{month:02d}"
        link = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year_month}.parquet"
        
        # Récupérez le nom du fichier à partir de l'URL
        file_name = os.path.join(yellowT, f"yellow_tripdata_{year_month}.parquet")
        
        try:
            # Téléchargez le fichier
            response = request.urlopen(link)
            with open(file_name, 'wb') as file:
                file.write(response.read())
            print(f"Le fichier {file_name} a été téléchargé avec succès.")
        except Exception as e:
            print(f"Erreur lors du téléchargement de {file_name}: {str(e)}")

"""

# import des packages du dernier mois
import urllib.request as request
import os
from datetime import datetime, timedelta

# Dossier local pour enregistrer le fichier téléchargé
yellowT = r"C:\ecole I1\ARCHITECTURE DECISIONNELLE\ATL-Datamart-main\ATL-Datamart-main\data\raw"

# Assurez-vous que le dossier local existe
if not os.path.exists(yellowT):
    os.mkdir(yellowT)

# Obtenez la date actuelle
current_date = datetime.now()

# Soustrayez un mois à la date actuelle
previous_month = current_date - timedelta(days=current_date.day)

# Obtenez l'année et le mois du mois précédent
previous_year = previous_month.year
previous_month = previous_month.month

# Formattez l'année et le mois du mois précédent pour correspondre au modèle de lien
previous_year_month = f"{previous_year}-{previous_month:02d}"

# Générez l'URL de téléchargement pour le package correspondant au mois précédent
link = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{previous_year_month}.parquet"

# Récupérez le nom du fichier à partir de l'URL
file_name = os.path.join(yellowT, f"yellow_tripdata_{previous_year_month}.parquet")

try:
    # Téléchargez le fichier
    response = request.urlopen(link)
    with open(file_name, 'wb') as file:
        file.write(response.read())
    print(f"Le fichier {file_name} a été téléchargé avec succès.")
except Exception as e:
    print(f"Erreur lors du téléchargement de {file_name}: {str(e)}")
"""