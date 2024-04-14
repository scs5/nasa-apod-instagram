import requests
import base64

# Pull image data from NASA API
url = f"https://api.nasa.gov/planetary/apod?api_key=k4htQqwTLG5urLDjzHeHYlutVmkfFStJXe3bX7dC&hd=TRUE"
response = requests.get(url)
apod_data = response.json()

# Extracting image URL
image_url = apod_data['url']
print(image_url)

# Downloading the image
image_response = requests.get(image_url)
image_data = image_response.content

# Decode image data
decoded_image = base64.b64decode(image_data)

# Saving the image to local system
with open("apod_image.jpg", "wb") as file:
    file.write(decoded_image)