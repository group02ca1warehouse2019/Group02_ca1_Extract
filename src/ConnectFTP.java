import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

public class ConnectFTP {
	private FTPClient ftpClient;

//	1: connect Ftp
	private boolean connectFTPServer(String id) {
		boolean resultC = false;

		ftpClient = new FTPClient();
		String sql = "SELECT *  FROM [dbo].[data_file_configuaration]" + "where isActive = 'false'" + "AND id = '" + id
				+ "'";

		try {
			Connection connect = ConnectDatabase.getSQLServerConnection();
			PreparedStatement pStatement = connect.prepareStatement(sql);
			ResultSet result = pStatement.executeQuery();

			while (result.next()) {
				if (id.equals(result.getString(1).trim())) {
					String ftp_address = result.getString(2).trim();
					int port = result.getInt(3);
					int timeout = result.getInt(8);
					String userName = result.getString(4).trim();
					String password = result.getString(5).trim();

					System.out.println("-------connecting ftp server " + ftp_address);
					System.out.println(" port = " + port + ", user = " + userName + ", pass = " + password);

					// connect to ftp server
					ftpClient.setDefaultTimeout(timeout);
					try {
						ftpClient.connect(ftp_address, port);
						// run the passive mode command
						ftpClient.enterLocalPassiveMode();

						ftpClient.setSoTimeout(timeout);

						ftpClient.login(userName, password);
						ftpClient.setDataTimeout(timeout);
						System.out.println("connected");
						resultC = true;
					} catch (IOException ex) {
						System.out.println("Khong kết nối được ftp");
						ftpClient.logout();
						ftpClient.disconnect();

					}
				}
			}
		} catch (Exception e) {
			System.out.println("connect Database fail");
		}
		return resultC;
	}

//	 2: download file
	private void downloadFTPFile() {
		String sql = "SELECT * FROM [dbo].[data_file_logs]" + "where status = 'Ex'";
		try {
			Connection connect = ConnectDatabase.getSQLServerConnection();
			PreparedStatement pStatement = connect.prepareStatement(sql);
			ResultSet result = pStatement.executeQuery();
			while (result.next()) {
				System.out.println(result.getString(2));

				if (connectFTPServer(result.getString(2).trim()) == true) {

					OutputStream outputStream = null;
					boolean success = false;
					try {
						File downloadFile = new File(result.getString(12), result.getString(3));
						String path = result.getString(11) + result.getString(3);
						System.out.println(path);
						outputStream = new BufferedOutputStream(new FileOutputStream(downloadFile));
						// download file from FTP Server
						ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
						ftpClient.setBufferSize(1024 * 1024);
						success = ftpClient.retrieveFile(path, outputStream);
						System.out.println(success);

						outputStream.close();
						ftpClient.logout();
						ftpClient.disconnect();
					} catch (IOException e) {
						System.out.println("file không tồn tại");
					}

					if (success) {
						System.out.println("File " + result.getString(11) + result.getString(3)
								+ " has been downloaded successfully.");

					}

				} else {
					System.out.println("connect ftp not success");
				}
			}

		} catch (ClassNotFoundException | SQLException e) {
			System.out.println("connect database Faith");
		}

	}

	public static void main(String[] args) {
		new ConnectFTP().downloadFTPFile();
	}
}
