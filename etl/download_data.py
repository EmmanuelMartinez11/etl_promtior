import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# URL hardcodeada ya que siempre se descargará de la misma pagina
url = "https://catalog.data.gov/dataset/electric-vehicle-population-data"


    # Funcion que comprueba que la pagina esté activa para descargar el CSV
def check_page_status(url: str) -> bool:
    try:
        reponse = requests.get(url, timeout=30)
        if reponse.status_code == 200:
            print(f"Página accesible: {url}")
            return True
        else:
            print(f"Página retornó código {reponse.status_code}: {url}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"Error al conectar con la página: {e}")
        return False

status = check_page_status(url)



# Descarga todos los archivos CSV con data-format='csv' desde la página y los guarda en la carpeta output_dir.
def download_csv(url: str, output_dir: str = "data"):
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')
        ul_elements = soup.find_all('ul')
        href_list = []

        for ul in ul_elements:
            a_tag = ul.find('a', {'data-format': 'csv'})
            if a_tag and 'href' in a_tag.attrs:
                href_list.append(a_tag['href'])

        if not href_list:
            print("No se encontraron enlaces CSV en la página.")
            return

        os.makedirs(output_dir, exist_ok=True)

        for i, relative_url in enumerate(href_list):
            full_url = urljoin(url, relative_url)
            filename = f"electric_vehicles_{i+1}.csv"
            filepath = os.path.join(output_dir, filename)

            print(f"Descargando desde {full_url}...")
            csv_response = requests.get(full_url)
            csv_response.raise_for_status()

            with open(filepath, "wb") as f:
                f.write(csv_response.content)

            print(f"Archivo guardado: {filepath}")

    except Exception as e:
        print(f"Error al descargar CSVs: {e}")