🧙‍♂️ Grimoire Crawler & Text Extractor

🚀 Grimoire Crawler is a specialized web crawler and text extraction tool designed to scrape, download, and extract text from over 2,000 PDFs on english.grimoar.cz. The project leverages PDF text scraping, image processing, OCR, and modern vector storage solutions.

📜 Table of Contents
Features
How It Works

License
✨ Features
🕸 Web Scraping: Crawl and download over 2,000 PDFs from english.grimoar.cz.
📄 PDF Text Extraction: Directly scrape text from PDFs when possible.
🖼 Image Processing: Convert PDFs to images using ImageMagick if direct text scraping fails.
🧠 OCR: Perform OCR on images using Tesseract to extract text.
📚 Vector Store Creation: Build a vector store of the extracted text for advanced querying.
☁️ Cloud Storage: Automatically upload text and vector data to AWS S3 and activeloop.ai.
🛠 How It Works
Crawling the Site: The crawler scans all relevant pages on english.grimoar.cz to identify and download the PDFs.

PDF Processing:

If the PDF allows text extraction, the text is directly scraped.
If the PDF is image-based or restricted, it is converted to a series of JPG images using ImageMagick.
OCR:

Tesseract is employed to perform Optical Character Recognition (OCR) on the images to extract the text.
Vectorization:

Extracted text is vectorized and stored in a vector database for semantic search capabilities.
Cloud Upload:

All processed text and vectors are uploaded to AWS S3 for storage and activeloop.ai for advanced vector management.
