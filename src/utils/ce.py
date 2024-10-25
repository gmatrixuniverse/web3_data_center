import aiohttp
import ssl
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from PIL import Image
import io
import pytesseract
from abc import ABC, abstractmethod
import logging

logger = logging.getLogger(__name__)

class ExtractorStrategy(ABC):
    @abstractmethod
    async def preprocess(self, source):
        pass

    @abstractmethod
    async def extract(self, preprocessed_data, depth):
        pass


class TextExtractorStrategy(ExtractorStrategy):
    async def preprocess(self, source):
        return source

    async def extract(self, preprocessed_data, depth):
        return await ContentExtractor.extract_from_text(preprocessed_data, max_depth=depth)

class URLExtractorStrategy(ExtractorStrategy):
    async def preprocess(self, source):
        logger.info(f"Extracting from URL: {source}")
        try:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            proxy = "http://127.0.0.1:7890"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(source, ssl=ssl_context, timeout=30, proxy=proxy) as response:
                    html_content = await response.text()

            soup = BeautifulSoup(html_content, 'html.parser')
            text_content = soup.get_text()

            # Extract links and resolve relative URLs
            links = []
            for a in soup.find_all('a', href=True):
                href = a['href']
                full_url = urljoin(source, href)
                links.append(full_url)

            # Extract iframe sources
            for iframe in soup.find_all('iframe', src=True):
                iframe_url = urljoin(source, iframe['src'])
                links.append(iframe_url)

            return {
                'text_content': text_content,
                'links': links
            }
        except Exception as e:
            logger.error(f"Error extracting from URL {source}: {str(e)}")
            return {'text_content': '', 'links': []}

    async def extract(self, preprocessed_data, depth):
        extracted_data = await ContentExtractor.extract_from_text(preprocessed_data['text_content'])
        
        # Add extracted links to the websites list
        extracted_data['websites'].extend(preprocessed_data['links'])

        # Remove duplicates
        for key in extracted_data:
            extracted_data[key] = list(set(extracted_data[key]))

        if depth > 1:
            await ContentExtractor._recursive_extract(extracted_data, depth - 1, set())

        return extracted_data

class ImageExtractorStrategy(ExtractorStrategy):
    async def preprocess(self, source):
        logger.info("Extracting from image")
        try:
            # Convert image data to PIL Image
            image = Image.open(io.BytesIO(source))

            # Perform OCR directly on the image
            text = pytesseract.image_to_string(image)

            return text
        except Exception as e:
            logger.error(f"Error extracting from image: {str(e)}")
            return ''

    async def extract(self, preprocessed_data, depth):
        return await ContentExtractor.extract_from_text(preprocessed_data, max_depth=depth)

class ContentExtractor:
    @classmethod
    async def extract_from_source(cls, source, strategy: ExtractorStrategy, depth=1):
        preprocessed_data = await strategy.preprocess(source)
        extracted_data = await strategy.extract(preprocessed_data, depth)
        return cls._postprocess(extracted_data)

    @staticmethod
    def _postprocess(extracted_data):
        # Remove duplicates
        for key in extracted_data:
            extracted_data[key] = list(set(extracted_data[key]))
        return extracted_data

    # Existing methods like extract_from_text, _recursive_extract, etc. remain unchanged
    # ...

# Usage example:
async def main():
    extractor = ContentExtractor()
    text_strategy = TextExtractorStrategy()
    url_strategy = URLExtractorStrategy()
    image_strategy = ImageExtractorStrategy()

    text_result = await extractor.extract_from_source("Sample text with $TICKER and 0x1234567890123456789012345678901234567890", text_strategy)
    url_result = await extractor.extract_from_source("https://example.com", url_strategy)
    
    # # Assuming you have image data in bytes
    # with open('path_to_image.jpg', 'rb') as image_file:
    #     image_data = image_file.read()
    # image_result = await extractor.extract_from_source(image_data, image_strategy)

    print("Text Result:", text_result)
    print("URL Result:", url_result)
    # print("Image Result:", image_result)