import io
import json
import logging

import azure.functions as func
from azure.storage.blob import BlobServiceClient
import os
from azure.ai.formrecognizer import DocumentAnalysisClient
import nltk
from bs4 import BeautifulSoup
from unstructured.partition.text import partition_text
from unstructured.partition.ppt import partition_ppt
from unstructured.partition.pptx import partition_pptx
from unstructured.partition.csv import partition_csv
from unstructured.partition.xlsx import partition_xlsx
from unstructured.partition.ppt import partition_ppt
from unstructured.partition.xml import partition_xml
from unstructured.partition.docx import partition_docx
from azure.core.credentials import AzureKeyCredential
from chunkdata import chunk_text
from createembedding import get_embedding_with_retry
from openai import OpenAI,AzureOpenAI
from insert_update_delete import update_or_insert_document
from azure.search.documents import SearchClient
import urllib

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
# Download NLTK punkt data for sentence tokenization
nltk.download('punkt')

CONTAINERNAME = os.getenv("CONTAINERNAME")
AZURE_STORAGE_CONNECTION_STRING=os.getenv("AzureWebJobsStorage")
FORM_RECOGNIZER_ENDPOINT = os.getenv("FORM_RECOGNIZER_ENDPOINT")
FORM_RECOGNIZER_KEY = os.getenv("FORM_RECOGNIZER_KEY")
EMBEDDING_MODEL = "text-embedding-ada-002"
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
SEARCHAPIENDPOINT = os.getenv("SEARCHAPIENDPOINT")
SEARCHAPIKEY = os.getenv("SEARCHAPIKEY")
INDEX_NAME = os.getenv("INDEX_NAME")


client = AzureOpenAI(
  api_key = AZURE_OPENAI_API_KEY,  
  api_version = "2023-05-15",
  azure_endpoint =AZURE_OPENAI_ENDPOINT 
)

@app.route(route="insert_blob_trigger")
def insert_blob_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:

        blobfile = req.files.getlist('files')
        if not blobfile:
                return func.HttpResponse("No file uploaded", status_code=400)

        
        # Azure Blob Storage connection
        connect_str = os.getenv('AzureWebJobsStorage')
        # Azure Blob Storage container name
        container_name = CONTAINERNAME

        # Creating the blob service client
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        container_client = blob_service_client.get_container_client(container_name)

        uploaded_files = []

        for file in blobfile:
            blob_client = container_client.get_blob_client(file.filename)
            blob_client.upload_blob(file.stream.read(), overwrite=True)
            uploaded_files.append(file.filename)

        return func.HttpResponse(
            f"Uploaded files: {', '.join(uploaded_files)}", 
            status_code=200
        )
    
    except Exception as e:
        logging.exception("Error uploading file")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)


@app.blob_trigger(arg_name="myblob", path="%CONTAINERNAME%/{name}",
                               connection="AzureWebJobsStorage") 
def BlobTrigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    try:
        meta_data_kb = ""
        blob_name = myblob.name
        document_client = None
        blob_client = None
        blob_client_n = None
        blob_service_client = None
        blob_client_metadata = None
        search_client = None
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=CONTAINERNAME, blob=myblob.name)  
    
        container_client = blob_service_client.get_container_client(CONTAINERNAME)
        substring = "_at"
        substringmetadata = "metadata"
        matching_blobs = []
        urlArr = urllib.parse.unquote(blob_client.url).split('/')
        if not substring in urlArr[5] and substringmetadata not in urlArr[5]:
            bfileName,ext = os.path.splitext(myblob.name)
            bfileNameArr = bfileName.split('/')
            
            for b in container_client.list_blobs():
                if bfileNameArr[1] in b.name:
                    matching_blobs.append(b.name)
            print(matching_blobs)

        
            for i in range(len(matching_blobs)):
                blob_client_n = blob_service_client.get_blob_client(container=CONTAINERNAME, blob=matching_blobs[i])
                properties = blob_client_n.get_blob_properties()
                content_type = properties.content_settings.content_type
            
                document_client = DocumentAnalysisClient(FORM_RECOGNIZER_ENDPOINT, AzureKeyCredential(FORM_RECOGNIZER_KEY))
                blob_data = blob_client_n.download_blob().readall()
                logging.info("Blob data (first 500 chars): %s", blob_data[:500])
                if len(matching_blobs) > 1:
                    blob_client_metadata = blob_service_client.get_blob_client(container=CONTAINERNAME, blob=matching_blobs[0])
                    metaData = blob_client_metadata.download_blob().readall()
                    json_meta_data = json.loads(metaData)
                    meta_data_kb = json_meta_data

                # if substringmetadata in matching_blobs[i]:
                #     blob_client_metadata = blob_service_client.get_blob_client(container=CONTAINERNAME, blob=matching_blobs[i])
                #     metaData = blob_client_metadata.download_blob().readall()
                #     json_meta_data = json.loads(metaData)
                #     meta_data_kb = json_meta_data


                if(content_type == "application/pdf"):
                    try:
                        content = ""
                        poller = document_client.begin_analyze_document("prebuilt-layout", blob_data)
                        result = poller.result()                     
                        for page in result.pages:
                            for line in page.lines:            
                                content += line.content +  " "
                       
                    except Exception as ex:
                        print("Exception occured for pdf ",ex)
                
                

                if(content_type == "image/jpeg" or content_type == "image/png" or content_type == "image/gif"):
                    try:
                        content = ""
                        poller = document_client.begin_analyze_document("prebuilt-layout", blob_data)
                        result = poller.result()
                        for page in result.pages:
                            for line in page.lines:            
                                content += line.content +  " "
                     
                    except Exception as ex:
                        print("Exception occured for images ",ex)
                    

                if(content_type == "text/plain" or content_type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document"):
                    try:
                       
                        content = ""
                        if blob_name.endswith(".txt"):
                            logging.info("Inside text reading block")
                            clean_content = preserve_links_html(blob_data)
                            
                            logging.info(f"Type of clean_content: {type(clean_content)}")
                            logging.info(f"Content of clean_content: {clean_content[:100]}") # Log first 100 characters
                            file_like_object = io.StringIO(clean_content)
                            # Log the type of file_like_object
                            logging.info(f"Type of file_like_object: {type(file_like_object)}")

                            
                            #content = ""
                            if not substring in matching_blobs[i]:    
                                logging.info("Inside if block")                
                               
                                try:
                                    elements = partition_text(file=stringio_to_bytes(file_like_object))
                                    logging.info("Number of elements from partition_text: %d", len(elements))
                                    content = "\n".join(str(el) for el in elements)
                                    logging.info("Final content after partition: %s", content[:500])
                                except Exception as ex:
                                    logging.exception("Unhandled exception in BlobTrigger: %s", str(ex))
                                    lines = clean_content.splitlines()
                                    content = "\n".join(line.strip() for line in lines if line.strip())
                                    logging.info("Before content is logged")
                                    logging.info("Content text or docx: %s", content)
                                    logging.info("After content is logged")


                            else:#for the attachments  
                                try: 
                                    elements = partition_text(file=blob_data)
                                    content = "\n".join(str(el) for el in elements)
                                except Exception as ex:
                                    lines = clean_content.splitlines()
                                    content = "\n".join(line.strip() for line in lines if line.strip())
                                    logging.info("Before content is logged")
                                    logging.info("Content text or docx: %s", content)
                                    logging.info("After content is logged")
                            
                        if  blob_name.lower().endswith(".docx"):
                            #content = ""
                            if not substring in matching_blobs[i]:             
                                content_stream = io.BytesIO(blob_data)
                                elements = partition_docx(file=content_stream)
                                htmlcontent = [BeautifulSoup(el.text, 'html.parser') for el in elements]    
                                content = "\n".join(soup.get_text(separator="\n") for soup in htmlcontent)
                            else: #for the attachments   
                                content_stream = io.BytesIO(blob_data)
                                elements = partition_docx(file=content_stream)
                                content =  "\n".join([str(el) for el in elements])
                      
                        logging.info("Before content is logged")
                        logging.info("Content text or docx: %s", content)
                        logging.info("After content is logged")
                    except Exception as ex:
                        print("Exception occured for text or plain ",ex)
                        
                
            
                if(content_type == "application/vnd.ms-powerpoint" or content_type == "application/vnd.openxmlformats-officedocument.presentationml.presentation"):
                    try:
                        content = ""
                        if blob_name.lower().endswith(".ppt"):
                            content_stream = io.BytesIO(blob_data)
                            elements = partition_ppt(file=content_stream)
                            content = "\n".join(str(el) for el in elements)
                        if blob_name.lower().endswith(".pptx"):
                            content_stream = io.BytesIO(blob_data)
                            elements = partition_pptx(file=content_stream)
                            content = "\n".join(str(el) for el in elements)
                    except Exception as ex:
                        print("Exception occured for powerpoint ",ex)
                    
                

                if(content_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" or content_type == "text/csv"):
                    try:
                        content = ""
                        if blob_name.lower().endswith(".csv"):
                            elements = partition_csv(file=blob_data)
                            content = "\n".join(str(el) for el in elements)
                        if blob_name.lower().endswith(".xlsx"):
                            elements = partition_xlsx(file=blob_data)
                            content = "\n".join(str(el) for el in elements)
                    except Exception as ex:
                        print("Exception occured for spreadsheetml ",ex)
                    
            

                if(content_type == "application/xml"):
                    try:
                        content = ""
                        elements = partition_xml(file=blob_data)
                        content = "\n".join(str(el) for el in elements)
                    except Exception as ex:
                        print("Exception occured for xml ",ex)

                if substringmetadata not in matching_blobs[i]:
                    response = chunk_text(text = content)
                    embedd_response = None
                    search_client = SearchClient(endpoint=SEARCHAPIENDPOINT,index_name=INDEX_NAME,credential = AzureKeyCredential(SEARCHAPIKEY))
                    logging.info("Search client created")
                    logging.info("Before content is logged")
                    logging.info("Content text or docx: %s", content)
                    logging.info("After content is logged")
                    if response:
                        try:
                            logging.info("Chunk Response created")
                            for chunk in response:  
                                embedd_response = get_embedding_with_retry(client, chunk, EMBEDDING_MODEL)
                            if embedd_response is None:
                                raise Exception("Fallback client embedding failed.")
                        except Exception as ex:
                            logging.info("Other region client created for embedding.")
                                
                    blob_file_name,extname = os.path.splitext(matching_blobs[i])
                    logging.info("Before content is logged")
                    logging.info("Content text or docx: %s", content)
                    logging.info("After content is logged")
                    logging.info(meta_data_kb)
                    document = {
                
                    "chunk_id": blob_file_name,
                    "content": content,
                    "vector": [] if embedd_response is None else embedd_response,    
                    "url" : meta_data_kb["Url"],
                    "short_description" : meta_data_kb["short_description"],
                    "author" : meta_data_kb["author"],
                    "language" : meta_data_kb["language"],
                    "number" : meta_data_kb["number"],
                    "sysID" : meta_data_kb["sysID"]
                
                    }
                    
                    update_or_insert_document(client=search_client,doc_id=blob_file_name ,doc_body=[document])
                    
    except Exception as ex:
        print("Exception occoured ", ex)
         
    finally:
        if blob_client is not None:
            blob_client.close()
        if blob_client_n is not None:
            blob_client_n.close()
        if blob_service_client is not None:
            blob_service_client.close()
        if search_client is not None:
            search_client.close()
        if document_client is not None:
            document_client.close()
        if blob_client_metadata is not None:
            blob_client_metadata.close()


def clean_html_text(blob_data):

    # Clean HTML content using BeautifulSoup
    soup = BeautifulSoup(blob_data, 'html.parser')
    return soup.get_text(separator="\n", strip=True)


def preserve_links_html(blob_data):
    soup = BeautifulSoup(blob_data, 'html.parser')
    output = []

    for element in soup.recursiveChildGenerator():
        if element.name == 'a':
            link_text = element.get_text()
            href = element.get('href')
            output.append(f"[{link_text}]({href})")
        elif element.name is None:
            output.append(element.strip() if isinstance(element, str) else "")
        elif element.name not in ['script', 'style']:  # Skip scripts/styles
            text = element.get_text(strip=True)
            if text:
                output.append(text)
    
    return "\n".join(output)

import io

def stringio_to_bytes(stringio_obj):
    # Move the cursor to the beginning of the StringIO object
    stringio_obj.seek(0)
    
    # Read the content of the StringIO object
    content = stringio_obj.read()
    
    # Convert the content to bytes
    bytes_content = content.encode('utf-8')
    
    return bytes_content


