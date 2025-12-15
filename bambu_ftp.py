"""
Bambu FTP Upload Module for PrintQue - Fixed Version with Correct FTP Sequence
Handles file uploads to Bambu printers via FTPS on port 990
"""
import ftplib
import ssl
import os
import socket
import logging
import time
import re
from typing import Optional, Tuple
from state import decrypt_api_key

logger = logging.getLogger(__name__)

class BambuImplicitFTPS(ftplib.FTP_TLS):
    """Custom FTPS class for Bambu's implicit FTPS on port 990 with session reuse"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._is_connected = False
        self.timeout = 30  # Set default timeout for all operations
        
    def connect(self, host='', port=0, timeout=-999, source_address=None):
        """Connect to host:port using implicit FTPS"""
        if host != '':
            self.host = host
        if port > 0:
            self.port = port
        if timeout != -999:
            self.timeout = timeout
        if source_address is not None:
            self.source_address = source_address
            
        # Create and wrap socket immediately for implicit FTPS
        self.sock = socket.create_connection((self.host, self.port), self.timeout, source_address=self.source_address)
        self.af = self.sock.family
        
        # Configure SSL context for TLS 1.2
        if not hasattr(self, 'context'):
            self.context = ssl.create_default_context()
        self.context.check_hostname = False
        self.context.verify_mode = ssl.CERT_NONE
        self.context.minimum_version = ssl.TLSVersion.TLSv1_2
        self.context.maximum_version = ssl.TLSVersion.TLSv1_2
        
        # Wrap with SSL immediately (implicit FTPS)
        self.sock = self.context.wrap_socket(self.sock, server_hostname=self.host)
        self._is_connected = True
        
        # Set up file for responses
        self.file = self.sock.makefile('r', encoding='latin-1')
        self.welcome = self.getresp()
        
        logger.debug(f"Connected to Bambu FTP: {self.welcome}")
        return self.welcome
    
    def ntransfercmd(self, cmd, rest=None):
        """Override to support SSL session reuse on data connection"""
        conn, size = super().ntransfercmd(cmd, rest)
        if self._prot_p:
            conn = self.context.wrap_socket(
                conn,
                server_hostname=self.host,
                session=self.sock.session  # Reuse control channel session
            )
        return conn, size

def upload_to_bambu(printer: dict, local_file: str, remote_name: Optional[str] = None) -> Tuple[bool, str]:
    """
    Upload a file to Bambu printer via FTPS with raw socket implementation and SSL session reuse
    
    Args:
        printer: Printer dictionary containing ip and access_code
        local_file: Path to local file to upload
        remote_name: Optional remote filename (defaults to basename with .gcode.3mf extension)
        
    Returns:
        Tuple of (success: bool, message: str)
    """
    printer_ip = printer['ip']
    printer_name = printer['name']
    
    # Decrypt access code
    try:
        access_code = decrypt_api_key(printer['access_code'])
        if not access_code:
            return False, "Failed to decrypt access code"
    except Exception as e:
        logger.error(f"Error decrypting access code: {str(e)}")
        return False, f"Failed to decrypt access code: {str(e)}"
    
    logger.info(f"Starting FTP upload to Bambu printer {printer_name} at {printer_ip}")
    
    # Check file exists
    if not os.path.exists(local_file):
        error_msg = f"File not found: {local_file}"
        logger.error(error_msg)
        return False, error_msg
    
    # Determine remote filename
    if remote_name is None:
        filename = os.path.basename(local_file)
        # Convert .gcode or .3mf to .gcode.3mf format
        if filename.endswith('.gcode'):
            remote_name = filename + '.3mf'
        elif filename.endswith('.3mf') and not filename.endswith('.gcode.3mf'):
            remote_name = filename[:-4] + '.gcode.3mf'
        else:
            remote_name = filename
    
    logger.debug(f"Local file: {local_file}, Remote name: {remote_name}")
    
    # Now use raw socket implementation with session reuse
    try:
        # Connect with SSL
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(30)
        
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.maximum_version = ssl.TLSVersion.TLSv1_2
        
        logger.debug(f"Connecting to {printer_ip}:990...")
        sock.connect((printer_ip, 990))
        ssock = context.wrap_socket(sock, server_hostname=printer_ip)
        
        file = ssock.makefile('rb')
        
        def send_cmd(cmd):
            logger.debug(f"Sending: {cmd.strip()}")
            ssock.send(cmd.encode('latin-1') + b'\r\n')
            response = file.readline().decode('latin-1').strip()
            logger.debug(f"Received: {response}")
            return response
        
        def send_without_read(cmd):
            logger.debug(f"Sending without immediate read: {cmd.strip()}")
            ssock.send(cmd.encode('latin-1') + b'\r\n')
            
        # Read welcome
        welcome = file.readline().decode('latin-1').strip()
        logger.debug(f"Received: {welcome}")
        
        # Login sequence
        user_resp = send_cmd('USER bblp')
        if not user_resp.startswith('331'):
            raise Exception(f"USER command failed: {user_resp}")
            
        pass_resp = send_cmd(f'PASS {access_code}')
        if not pass_resp.startswith('230'):
            raise Exception(f"Login failed: {pass_resp}")
            
        logger.info(f"Successfully logged into Bambu printer {printer_name}")
        
        # Set data protection to private (encrypted)
        prot_resp = send_cmd('PROT P')
        if not prot_resp.startswith('200'):
            raise Exception(f"PROT P failed: {prot_resp}")
        logger.debug("Data protection set to private (encrypted)")
        
        # Set binary mode
        type_resp = send_cmd('TYPE I')
        if not type_resp.startswith('200'):
            raise Exception(f"TYPE I failed: {type_resp}")
        
        # Enter passive mode
        pasv_response = send_cmd('PASV')
        
        # Parse PASV response (227 Entering Passive Mode (h1,h2,h3,h4,p1,p2))
        if pasv_response.startswith('227'):
            match = re.search(r'\((\d+),(\d+),(\d+),(\d+),(\d+),(\d+)\)', pasv_response)
            if match:
                data_host = '.'.join(match.groups()[:4])
                data_port = int(match.group(5)) * 256 + int(match.group(6))
                logger.debug(f"Data connection: {data_host}:{data_port}")
                
                # Send STOR without reading response yet
                send_without_read(f'STOR {remote_name}')
                
                # NOW open data connection
                data_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                data_sock.settimeout(30)
                data_sock.connect((data_host, data_port))
                
                # Wrap data socket with SSL, reusing session from control connection
                data_sock = context.wrap_socket(
                    data_sock,
                    server_hostname=printer_ip,
                    session=ssock.session  # Reuse SSL session
                )
                
                # Now read the "150" response
                stor_resp = file.readline().decode('latin-1').strip()
                logger.debug(f"STOR response: {stor_resp}")
                if not stor_resp.startswith('150'):
                    raise Exception(f"STOR command failed: {stor_resp}")
                
                # Send file data
                file_size = os.path.getsize(local_file)
                logger.info(f"Uploading {os.path.basename(local_file)} ({file_size:,} bytes) to {printer_name}...")
                
                with open(local_file, 'rb') as f:
                    sent = 0
                    start_time = time.time()
                    while True:
                        chunk = f.read(8192)
                        if not chunk:
                            break
                        data_sock.send(chunk)
                        sent += len(chunk)
                        if sent % (1024 * 256) < len(chunk):  # Log every 256KB
                            percent = (sent / file_size) * 100
                            logger.debug(f"Upload progress: {percent:.1f}% ({sent:,}/{file_size:,} bytes)")
                
                data_sock.close()
                upload_time = time.time() - start_time
                
                # Get final response
                final_response = file.readline().decode('latin-1').strip()
                logger.debug(f"Received: {final_response}")
                
                if final_response.startswith('226'):
                    logger.info(f"Upload successful! Transferred {file_size:,} bytes in {upload_time:.1f} seconds")
                    
                    # Try to verify (optional)
                    try:
                        size_resp = send_cmd(f'SIZE {remote_name}')
                        if size_resp.startswith('213'):
                            reported_size = int(size_resp.split()[1])
                            logger.debug(f"Verified: {remote_name} ({reported_size:,} bytes) on printer")
                    except:
                        logger.debug("Couldn't verify file size (this is often normal)")
                    
                    # Clean disconnect
                    try:
                        send_cmd('QUIT')
                    except:
                        pass
                        
                    file.close()
                    ssock.close()
                    
                    success_msg = f"Successfully uploaded {remote_name} to {printer_name}"
                    logger.info(success_msg)
                    return True, success_msg
                else:
                    raise Exception(f"Upload failed: {final_response}")
            else:
                raise Exception(f"Could not parse PASV response: {pasv_response}")
        else:
            raise Exception(f"PASV command failed: {pasv_response}")
                    
    except Exception as e:
        error_msg = f"FTP upload error: {str(e)}"
        logger.error(f"Error uploading to {printer_name}: {error_msg}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        # Attempt to clean up connections
        try:
            file.close()
            ssock.close()
        except:
            pass
        return False, error_msg

def prepare_gcode_for_bambu(filepath: str, upload_folder: str) -> Tuple[bool, str, str]:
    """
    Prepare a G-code file for Bambu printer upload
    
    Args:
        filepath: Path to the original file
        upload_folder: Folder to store prepared files
        
    Returns:
        Tuple of (success: bool, prepared_filepath: str, remote_filename: str)
    """
    try:
        filename = os.path.basename(filepath)
        logger.debug(f"Preparing file for Bambu: {filename}")
        
        # Determine the remote filename
        if filename.endswith('.gcode'):
            # Remove .gcode and add .gcode.3mf
            base = filename[:-6]  # Remove .gcode
            remote_filename = f"{base}.gcode.3mf"
        elif filename.endswith('.3mf') and not filename.endswith('.gcode.3mf'):
            # Convert .3mf to .gcode.3mf
            base = filename[:-4]  # Remove .3mf
            remote_filename = f"{base}.gcode.3mf"
        elif filename.endswith('.gcode.3mf'):
            # Already in correct format
            remote_filename = filename
        else:
            # For any other format, append .gcode.3mf
            remote_filename = f"{filename}.gcode.3mf"
            
        # For now, we'll use the original file
        # In the future, you might want to convert or package the file here
        prepared_filepath = filepath
        
        logger.debug(f"Prepared file for Bambu: {prepared_filepath} -> {remote_filename}")
        return True, prepared_filepath, remote_filename
        
    except Exception as e:
        logger.error(f"Error preparing file for Bambu: {str(e)}")
        return False, "", ""