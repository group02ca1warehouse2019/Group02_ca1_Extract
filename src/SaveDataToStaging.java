import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Date;

public class SaveDataToStaging {

//	2: save data to staging
	public void downloadFiletoFTP() throws IOException {
 
		String sql = " SELECT  dl.* , df.sql_insert_staging FROM [dbo].[data_file_logs] dl join  [dbo].[data_file_configuaration] df on dl.id_config = df.id  where dl.status = 'Ex' and dl.id =5";
		String sqlUpdate = "Update [dbo].[data_file_logs] set [status] = 'Er', date_up_staging = ? where id = ? " ;
		try {
			Connection connect = ConnectDatabase.getSQLServerConnection();
			
			PreparedStatement pStatement = connect.prepareStatement(sql);

			ResultSet result = pStatement.executeQuery();
		System.out.println("ok");
			while (result.next()) {
				int id = result.getInt(1);
//				String id_config = result.getString(2).trim();
				String file_name = result.getString(3).trim();
				System.out.println("----------Insert Table Student" + id + " ------------");
				System.out.println(file_name);
//				String status = result.getString(4).trim();
				int size_file = result.getInt(5);
//				String time = result.getString(6);
				String delimiter = result.getString(7).trim();
				System.out.println("====>delimiter " + delimiter);
				String charset = result.getString(8).trim();
				String istitile = result.getString(9).trim();
				String sql_ = result.getString(17);
				int nRow = result.getInt(10);
				String dest_ = result.getString(12).trim();
				int nCol = result.getInt(13);
				String table = result.getString(14).trim();

//					copy file
				File file = new File(dest_, file_name);
				if (file.length() < size_file) {
					System.out.println("file download to local not success");
				}
				BufferedReader input = new BufferedReader(new InputStreamReader(new FileInputStream(file), charset));
				String line = "";
				Connection conn = ConnectDatabase.getSQLServerConnectionStaging();
				String sql2 = "INSERT INTO " + table + " " + sql_;
				System.out.println(sql2);
				System.out.println(nCol + "");
				int sum = 0;
				if (istitile.equalsIgnoreCase("yes")) {
					System.out.println(input.readLine());
				}
				while ((line = input.readLine()) != null) {
					try {
						PreparedStatement pstm = conn.prepareStatement(sql2);

					
						String[] s = line.split(delimiter);
						for (int i = 0; i < nCol; i++) {

//							System.out.println(s[i]);
							pstm.setString(i + 1, s[i].trim());
						}

						pstm.setInt(nCol + 1, id);

						sum += pstm.executeUpdate();
					} catch (Exception e) {
						System.out.println(line + "\t" + id);
						System.out.println("cap nhat that bại");
					}
				}

				if (sum == nRow) {
					 Calendar cal = Calendar.getInstance();
					System.out.println("Chen thanh cong " + sum + " hang");
					PreparedStatement sUpdate = connect.prepareStatement(sqlUpdate );
					sUpdate.setString(1,(cal.get(Calendar.YEAR) +"/"+ (cal.get(Calendar.MONTH)+1) +"/"+cal.get(Calendar.DAY_OF_MONTH)));
					sUpdate.setInt(2,id );
//					Statement sm = connect.createStatement();
//					sm.execute(sqlUpdate +id);
							
					sUpdate.executeUpdate();
				} else {
					System.out.println("chi chen duoc " + sum + " row, fail " + (nRow - sum) + " row");
				}

			}

		} catch (ClassNotFoundException | SQLException e) {
			System.out.println("connect database Faith");
		}

	}

	public static void main(String[] args) throws IOException {
		new SaveDataToStaging().downloadFiletoFTP();
	}

}
