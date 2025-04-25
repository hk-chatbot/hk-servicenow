import io
import json
import logging
import uuid
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import os
from azure.ai.formrecognizer import DocumentAnalysisClient
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

client = OpenAI(api_key=os.getenv("openai_api_key"))
client = AzureOpenAI(
  api_key = AZURE_OPENAI_API_KEY,  
  api_version = "2024-02-01",
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


@app.blob_trigger(arg_name="myblob", path="servicenow-container/{name}",
                               connection="AzureWebJobsStorage") 
def BlobTrigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    try:
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
                
                logging.debug(f'Blobdata: {blob_data}')

                if substringmetadata in matching_blobs[i]:
                    blob_client_metadata = blob_service_client.get_blob_client(container=CONTAINERNAME, blob=matching_blobs[i])
                    metaData = blob_client_metadata.download_blob().readall()
                    json_meta_data = json.loads(metaData)


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
                       
                      
                        if blob_name.endswith(".txt"):
                            logging.info("Inside text reading block")
                            content = ""
                            if not substring in matching_blobs[i]:                    
                                elements = partition_text(file=blob_data)
                                htmlcontent = [BeautifulSoup(el.text, 'html.parser') for el in elements]
                                content = "\n".join(soup.get_text(separator="\n") for soup in htmlcontent)
                                # soup = [BeautifulSoup(el.text, 'html.parser') for el in elements]
                                # output = []    
                                # # Walk through all elements, preserving links
                                # for element in soup.recursiveChildGenerator():
                                #     if element.name == 'a':
                                #         link_text = element.get_text()
                                #         href = element.get('href')
                                #         output.append(f'[{link_text}]({href})')
                                #     elif element.name is None:
                                #         output.append(element)
                                # # Join with no extra whitespace
                                # final_text = ''.join(output)
                                # print(final_text)


                            else:#for the attachments   
                                elements = partition_text(file=blob_data)
                                content = "\n".join(str(el) for el in elements)
                            
                        if  blob_name.lower().endswith(".docx"):
                            content = ""
                            if not substring in matching_blobs[i]:             
                                content_stream = io.BytesIO(blob_data)
                                elements = partition_docx(file=content_stream)
                                htmlcontent = [BeautifulSoup(el.text, 'html.parser') for el in elements]    
                                content = "\n".join(soup.get_text(separator="\n") for soup in htmlcontent)
                            else: #for the attachments   
                                content_stream = io.BytesIO(blob_data)
                                elements = partition_docx(file=content_stream)
                                content =  "\n".join([str(el) for el in elements])
                      
                        #logging.info("Content text or docx: %s", content)
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
                    logging.info("content %s",content)
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
                    document = {
                
                    "chunk_id": blob_file_name,
                    "content": content,
                    "vector": [] if embedd_response is None else embedd_response                
                
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

