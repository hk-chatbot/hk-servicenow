from datetime import datetime

def update_or_insert_document(client, doc_id, doc_body):
    status = ""
    blobData = ""
    try:
        # First, try to retrieve the document by its ID
        results = client.get_document(key=doc_id)
        print(f"Document with ID {doc_id} found. Updating the document...")

        blobData = doc_body[0]

        # If the document exists, we update it using IndexBatch
        if results is not None and results != []:
            result = client.merge_documents(documents=[doc_body[0]])
            print("Document updated successfully.")
           
        else:
            print(f"Document with ID {doc_id} not found. Inserting a new document...")
            result = client.upload_documents(documents=[doc_body[0]])
            print("New document inserted successfully.")
           

    except Exception as e:
        # Document does not exist, so insert the document
        print(f"Document with ID {doc_id} not found. Inserting a new document...")
        try:
            result = client.upload_documents(documents=[doc_body[0]])
            print("New document inserted successfully.")
          
        except Exception as ex:
            print("Exception occured")
          
    finally:
        client.close()
      

      