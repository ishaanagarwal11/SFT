import shutil
import os
import logging

logging.basicConfig(
    level=logging.DEBUG,
    handlers=[
        logging.StreamHandler()  
    ]
)

def delete_data_folder():
    """
    Deletes the 'data' folder located in the root directory of the project and all of its contents.
    """
    root_path = os.path.dirname(os.path.abspath(__file__))
    root_path = os.path.abspath(os.path.join(root_path, '..'))
    logging.debug(f"Root path of the project: {root_path}")
    
    folder_path = os.path.join(root_path, "data")
    logging.debug(f"Full path to 'data' folder: {folder_path}")
    
    if os.path.exists(folder_path) and os.path.isdir(folder_path):
        try:
            logging.info(f"Attempting to delete folder: {folder_path}")
            shutil.rmtree(folder_path)  
            logging.info(f"Successfully deleted the folder: {folder_path}")
        except PermissionError:
            logging.error(f"Permission denied. You do not have the necessary permissions to delete {folder_path}.")
        except OSError as e:
            logging.error(f"An error occurred while deleting the folder: {e.strerror}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {str(e)}")
    else:
        logging.warning(f"The folder {folder_path} does not exist or is not a valid directory.")

