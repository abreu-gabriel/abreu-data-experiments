import requests

base_url = "https://olinda.bcb.gov.br/olinda/servico/Pix_DadosAbertos/versao/v1/odata"

# For OData functions, parameters go in the URL, not as query params
date = "2023-11-01"  # Pix started in Nov 2020, let's try a date we know has data
url = f"{base_url}/TransacoesPixPorMunicipio(DataBase='{date}')"

print(f"Full URL: {url}")

response = requests.get(url)

print(f"Status Code: {response.status_code}")

if response.status_code == 200:
    data = response.json()
    if data.get('value'):
        print(f"\nSuccess! Found {len(data['value'])} records")
        print(f"\nFirst record sample:")
        print(data['value'][0])
    else:
        print("Empty result")
        print(data)
else:
    print(f"Error: {response.text}")