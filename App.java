package com.sunyard.insurance.loanapp.ocr;
 
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
 
import javax.sql.DataSource;
  
import org.apache.log4j.Logger;
 
import com.sunyard.archives.bhaf.bean.Loan;
import com.sunyard.insurance.cbif.demo.html.DemoStartupServlet;
import com.sunyard.insurance.imaging.taskTransSteps.BatchVerifyInfo;
import com.sunyard.insurance.imaging.taskTransSteps.InvoiceInfo;
import com.sunyard.insurance.imaging.taskTransSteps.VerifyInfo;
import com.sunyard.insurance.util.CommonFunc;
import com.sunyard.wfproxy.sunflow.start.DBAccess;
 
public class CommonDaoImpl implements CommonDao {
      private static final Logger logger = Logger.getLogger(CommonDaoImpl.class);
      private static final DataSource IDSOURCE = DemoStartupServlet.interfaceDataSource;
      private static final DataSource SDSOURCE = DemoStartupServlet.dataSource;
      private Connection connection = null;
      private PreparedStatement preparedStatement = null;
      private ResultSet rs = null;
      private ResultSetMetaData rsmd = null;
 
              /**
        * ��ȡĬ�����ݿ�����
        *
        * @throws SQLException
        */
              private void openIDS() throws SQLException {
            try {
                  connection = IDSOURCE.getConnection();
            } catch (SQLException e) {
                  throw e;
            }
      }
 
              /**
        *
        *
        * @throws SQLException
        */
              private void openSDS() throws SQLException {
            try {
                  connection = SDSOURCE.getConnection();
            } catch (SQLException e) {
                  throw e;
            }
      }
 
              /**
        * ��ɾ��
        *
        * @throws Exception
        */
              private int executeUpdate(String sql, Object[] objs) throws SQLException {
            int flag = -1;
            try {
                  if (connection != null && !connection.getAutoCommit()) {
                        connection.setAutoCommit(true);
                  }
                  preparedStatement = connection.prepareStatement(sql);
                  if (objs != null && objs.length > 0) {
                        for (int i = 0; i < objs.length; i++) {
                              preparedStatement.setObject(i + 1, objs[i]);
                        }
                  }
                  flag = preparedStatement.executeUpdate();
                  return flag;
            } catch (SQLException e) {
                  try {
                        if (connection != null) {
                              logger.info("---------- ����ع� --------------");
                              connection.rollback();
                        }
                  } catch (SQLException e2) {
                        logger.error("����ع��쳣: " + e2.getMessage());
                  }
                  throw e;
            }
      }
 
              /**
        * ��ѯ
        *
        * @param sql
        * @param objs
        * @return
        * @throws Exception
        */
              private ResultSet executeQuery(String sql, Object[] objs)
                  throws SQLException {
            try {
                  if (connection != null && !connection.getAutoCommit()) {
                        connection.setAutoCommit(true);
                  }
                  preparedStatement = connection.prepareStatement(sql);
                  if (objs != null && objs.length > 0) {
                        for (int i = 0; i < objs.length; i++) {
                              preparedStatement.setObject(i + 1, objs[i]);
                        }
                  }
                  rs = preparedStatement.executeQuery();
                  return rs;
            } catch (SQLException e) {
                  throw e;
            }
      }
 
              /**
        * �ͷ����ݿ�����
        *
        * @throws Exception
        */
              private void closeDB() throws SQLException {
            try {
                  if (rs != null) {
                        rs.close();
                  }
                  if (preparedStatement != null) {
                        preparedStatement.close();
                  }
                  if (connection != null) {
                        connection.close();
                  }
            } catch (SQLException e) {
                  throw new SQLException("�ͷ����ݿ������쳣:" + e.getMessage(), e);
            }
      }
 
              /**
        * ����������Ʊʶ��/�ȶԼ�¼
        *
        */
              public int inserInvoice(VehicleInvoice invoice) throws SQLException {
            String sql = null;
            try {
                  sql = "insert into vehicle_invoice (image_id,application_id,create_date,invoice_date,invoice_no,borrower_name,borrower_id,engine_no,vin_no,amount,amount_number,dealer_name,car_model,time_cost,error_code,error_msg,check_invoice_date,check_borrower_name,check_borrower_id,check_vin_no,check_amount,check_amount_number,check_dealer_name,check_result) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
                  Object[] objs = { invoice.getImageId(), invoice.getApplicationId(),
                              invoice.getCreateDate(), invoice.getInvoiceDate(),
                              invoice.getInvoiceNo(), invoice.getBorrowerName(),
                              invoice.getBorrowerId(), invoice.getEngineNo(),
                              invoice.getVinNo(), invoice.getAmount(),
                              invoice.getAmountNumber(), invoice.getDealerName(),
                              invoice.getCarModel(),
                              invoice.getCostTime(), invoice.getErrorCode(),
                              invoice.getErrorMsg(), invoice.getCheckInvoiceDate(),
                              invoice.getCheckBorrowerName(),
                              invoice.getCheckBorrowerId(), invoice.getCheckVinNo(),
                              invoice.getCheckAmount(), invoice.getCheckAmountNumber(),
                              invoice.getCheckDealerName(), invoice.getCheckResult() };
                  if (isInvoiceExist(invoice)) {
                        return updateInvoice(invoice);
                  } else {
                        openIDS();
                        return executeUpdate(sql, objs);
                  }
            } catch (SQLException e) {
                  throw new SQLException("����������Ʊʶ��/�ȶԼ�¼,SQLִ���쳣:" + e.getMessage(), e);
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              /**
        * ��ѯ��¼�Ƿ����
        *
        * @return true:���м�¼,false:�޼�¼
        * @throws Exception
        */
              public boolean isInvoiceExist(VehicleInvoice invoice) throws SQLException {
            String sql = null;
            boolean isExist = false;
            try {
                  sql = "select count(*) is_exist from vehicle_invoice where image_id =?";
                  Object[] objs = { invoice.getImageId() };
                  openIDS();
                  executeQuery(sql, objs);
                  if (rs.next()) {
                        isExist = !"0".equals(rs.getString("is_exist"));
                  }
                  return isExist;
            } catch (SQLException e) {
                  throw e;
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              /**
        * ��ѯ������Ʊʶ��/�ȶԼ�¼
        *
        * @throws Exception
        */
              public VehicleInvoice selectInvoiceByImageId(String imageId)
                  throws SQLException {
            VehicleInvoice invoice = null;
            try {
                  String sql = "select image_id,application_id,create_date,invoice_date,invoice_no,borrower_name,borrower_id,engine_no,vin_no,amount,amount_number,dealer_name,time_cost,error_code,error_msg,check_invoice_date,check_borrower_name,check_borrower_id,check_vin_no,check_amount,check_amount_number,check_dealer_name,check_result from vehicle_invoice where image_id = ?";
                  Object[] objs = { imageId };
                  openIDS();
                  executeQuery(sql, objs);
                  if (rs.next()) {
                        invoice = new VehicleInvoice();
                        invoice.setImageId(rs.getString("image_id"));
                        invoice.setApplicationId(rs.getString("application_id"));
                        invoice.setCreateDate(rs.getTimestamp("create_date"));
                        invoice.setInvoiceDate(rs.getString("invoice_date"));
                        invoice.setInvoiceNo(rs.getString("invoice_no"));
                        invoice.setBorrowerName(rs.getString("borrower_name"));
                        invoice.setBorrowerId(rs.getString("borrower_id"));
                        invoice.setEngineNo(rs.getString("engine_no"));
                        invoice.setVinNo(rs.getString("vin_no"));
                        invoice.setAmount(rs.getString("amount"));
                        invoice.setAmountNumber(rs.getString("amount_number"));
                        invoice.setDealerName(rs.getString("dealer_name"));
                        invoice.setCostTime(rs.getInt("time_cost"));
                        invoice.setErrorCode(rs.getInt("error_code"));
                        invoice.setErrorMsg(rs.getString("error_msg"));
                        invoice.setCheckInvoiceDate(rs.getString("check_invoice_date"));
                        invoice.setCheckBorrowerName(rs
                                                            .getString("check_borrower_name"));
                        invoice.setCheckBorrowerId(rs.getString("check_borrower_id"));
                        invoice.setCheckVinNo(rs.getString("check_vin_no"));
                        invoice.setCheckAmount(rs.getString("check_amount"));
                        invoice.setCheckAmountNumber(rs
                                                            .getString("check_amount_number"));
                        invoice.setCheckDealerName(rs.getString("check_dealer_name"));
                        invoice.setCheckResult(rs.getString("check_result"));
                  }
                  return invoice;
            } catch (SQLException e) {
                  throw new SQLException("��ѯ������Ʊʶ��/�ȶԼ�¼,SQLִ���쳣" + e.getMessage(), e);
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              /**
        * ��ѯ������Ʊʶ��/�ȶԼ�¼
        *
        * @throws Exception
        */
              public VehicleInvoice selectInvoiceByApplicationId(String applicationId)
                  throws SQLException {
            VehicleInvoice invoice = null;
            try {
                  String sql = "select * from(select image_id,application_id,create_date,invoice_date,invoice_no,borrower_name,borrower_id,engine_no,vin_no,amount,amount_number,dealer_name,time_cost,error_code,error_msg,check_invoice_date,check_borrower_name,check_borrower_id,check_vin_no,check_amount,check_amount_number,check_dealer_name,check_result from vehicle_invoice where application_id = ? order by create_date DESC)where rownum = 1";
                  Object[] objs = { applicationId };
                  openIDS();
                  executeQuery(sql, objs);
                  if (rs.next()) {
                        invoice = new VehicleInvoice();
                        invoice.setImageId(rs.getString("image_id"));
                        invoice.setApplicationId(rs.getString("application_id"));
                        invoice.setCreateDate(rs.getTimestamp("create_date"));
                        invoice.setImageId(rs.getString("image_id"));
                        invoice.setInvoiceDate(rs.getString("invoice_date"));
                        invoice.setInvoiceNo(rs.getString("invoice_no"));
                        invoice.setBorrowerName(rs.getString("borrower_name"));
                        invoice.setBorrowerId(rs.getString("borrower_id"));
                        invoice.setEngineNo(rs.getString("engine_no"));
                        invoice.setVinNo(rs.getString("vin_no"));
                        invoice.setAmount(rs.getString("amount"));
                        invoice.setAmountNumber(rs.getString("amount_number"));
                        invoice.setDealerName(rs.getString("dealer_name"));
                        invoice.setCostTime(rs.getInt("time_cost"));
                        invoice.setErrorCode(rs.getInt("error_code"));
                        invoice.setErrorMsg(rs.getString("error_msg"));
                        invoice.setCheckInvoiceDate(rs.getString("check_invoice_date"));
                        invoice.setCheckBorrowerName(rs
                                                            .getString("check_borrower_name"));
                        invoice.setCheckBorrowerId(rs.getString("check_borrower_id"));
                        invoice.setCheckVinNo(rs.getString("check_vin_no"));
                        invoice.setCheckAmount(rs.getString("check_amount"));
                        invoice.setCheckAmountNumber(rs
                                                            .getString("check_amount_number"));
                        invoice.setCheckDealerName(rs.getString("check_dealer_name"));
                        invoice.setCheckResult(rs.getString("check_result"));
                  }
            } catch (SQLException e) {
                  throw new SQLException("��ѯ������Ʊʶ��/�ȶԼ�¼,SQLִ���쳣" + e.getMessage(), e);
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
            return invoice;
      }
 
              /**
        * ���¹�����Ʊʶ��/�ȶԼ�¼(ȫ����)
        *
        * @throws Exception
        */
              public int updateInvoice(VehicleInvoice invoice) throws SQLException {
            try {
                  String sql = "UPDATE vehicle_invoice SET application_id=?,create_date=?,invoice_date=?,invoice_no=?,borrower_name=?,borrower_id=?,engine_no=?,vin_no=?,amount=?,amount_number=?,dealer_name=?,time_cost=?,error_code=?,error_msg=?,check_invoice_date=?,check_borrower_name=?,check_borrower_id=?,check_vin_no=?,check_amount=?,check_amount_number=?,check_dealer_name=?,check_result=?"
                              + " where image_id=?";
                  Object[] objs = { invoice.getApplicationId(),
                              invoice.getCreateDate(), invoice.getInvoiceDate(),
                              invoice.getInvoiceNo(), invoice.getBorrowerName(),
                              invoice.getBorrowerId(), invoice.getEngineNo(),
                              invoice.getVinNo(), invoice.getAmount(),
                              invoice.getAmountNumber(), invoice.getDealerName(),
                              invoice.getCostTime(), invoice.getErrorCode(),
                              invoice.getErrorMsg(), invoice.getCheckInvoiceDate(),
                              invoice.getCheckBorrowerName(),
                              invoice.getCheckBorrowerId(), invoice.getCheckVinNo(),
                              invoice.getCheckAmount(), invoice.getCheckAmountNumber(),
                              invoice.getCheckDealerName(), invoice.getCheckResult(),
                              invoice.getImageId() };
                  openIDS();
                  return executeUpdate(sql, objs);
            } catch (SQLException e) {
                  throw new SQLException("���¹�����Ʊʶ��/�ȶԼ�¼ִ��SQL�����쳣: " + e.getMessage(),
                                                  e);
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              /**
        * ����������Ȩ��OCR�ȶԼ�¼
        *
        * @throws Exception
        */
              public int inserWarrant(Warrant warrant) throws SQLException {
            String sql = "insert into warrant (IMAGE_ID, APPLICATION_ID, CREATE_DATE, BANK_NAME, CUS_NAME, ACCOUNT_NAME, ACCOUNT_NUMBER, CUS_ID, SIGN_NAME, SIGN_DATE, LOGO, TIME_COST,ERROR_CODE,ERROR_MSG,CHECK_BANK_NAME, CHECK_CUS_NAME, CHECK_ACCOUNT_NAME, CHECK_ACCOUNT_NUMBER, CHECK_CUS_ID, CHECK_SIGN_NAME, CHECK_SIGN_DATE, CHECK_LOGO, CHECK_RESULT) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            Object[] objs = { warrant.getImageId(), warrant.getApplicationId(),
                        warrant.getCreateDate(), warrant.getBankName(),
                        warrant.getCusName(), warrant.getAccountName(),
                        warrant.getAccountNumber(), warrant.getCusId(),
                        warrant.getSignName(), warrant.getSignDate(),
                        warrant.getLogo(), warrant.getTimeCost(),
                        warrant.getErrorCode(), warrant.getErrorMsg(),
                        warrant.getCheckBankName(), warrant.getCheckCusName(),
                        warrant.getCheckAccountName(), warrant.getCheckAccountNumber(),
                        warrant.getCheckCusId(), warrant.getCheckSignName(),
                        warrant.getCheckSignDate(), warrant.getCheckLogo(),
                        warrant.getCheckResult() };
            try {
                  if (isWarrantExsit(warrant)) {
                        return updateWarrant(warrant);
                  } else {
                        openIDS();
                        return executeUpdate(sql, objs);
                  }
            } catch (SQLException e) {
                  throw new SQLException("����������Ȩ��OCR�ȶԼ�¼SQLִ���쳣�� " + e.getMessage(),
                                                  e);
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              /**
        * ��ѯ��¼�Ƿ����
        */
              public boolean isBankCardExsit(BankCard bankCard)throws SQLException{
            boolean isExist = false;
            try {
                  String sql = "select count(*) is_exist from bank_card where image_id =?";
                  Object[] objs = { bankCard.getImageId() };
                  openIDS();
                  executeQuery(sql, objs);
                  if (rs.next()) {
                        isExist = !"0".equals(rs.getString("is_exist"));
                  }
                  return isExist;
            } catch (SQLException e) {
                  throw e;
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
      /**
        * ��ѯ��¼�Ƿ����
        */
              public boolean isWarrantExsit(Warrant warrant) throws SQLException {
            boolean isExist = false;
            try {
                  String sql = "select count(*) is_exist from warrant where image_id =?";
                  Object[] objs = { warrant.getImageId() };
                  openIDS();
                  executeQuery(sql, objs);
                  if (rs.next()) {
                        isExist = !"0".equals(rs.getString("is_exist"));
                  }
                  return isExist;
            } catch (SQLException e) {
                  throw e;
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              /**
        * ��ѯ������Ȩ��OCR�ȶԼ�¼
        *
        * @throws Exception
        */
              public Warrant selectWarrantByApplicationId(String applicationId)
                  throws Exception {
            Warrant warrant = null;
            try {
                  String sql = "select * from(select * from warrant where application_id = ? order by create_date desc)where rownum = 1";
                  Object[] objs = { applicationId };
                  openIDS();
                  executeQuery(sql, objs);
                  if (rs.next()) {
                        warrant = new Warrant();
                        warrant.setImageId(rs.getString("IMAGE_ID"));
                        warrant.setApplicationId(rs.getString("APPLICATION_ID"));
                        warrant.setCreateDate(rs.getTimestamp("CREATE_DATE"));
                        warrant.setBankName(rs.getString("BANK_NAME"));
                        warrant.setCusName(rs.getString("CUS_NAME"));
                        warrant.setAccountName(rs.getString("ACCOUNT_NAME"));
                        warrant.setAccountNumber(rs.getString("ACCOUNT_NUMBER"));
                        warrant.setCusId(rs.getString("CUS_ID"));
                        warrant.setSignName(rs.getString("SIGN_NAME"));
                        warrant.setSignDate(rs.getString("SIGN_DATE"));
                        warrant.setLogo(rs.getString("LOGO"));
                        warrant.setTimeCost(rs.getInt("TIME_COST"));
                        warrant.setErrorCode(rs.getInt("ERROR_CODE"));
                        warrant.setErrorMsg(rs.getString("ERROR_MSG"));
                        warrant.setCheckBankName(rs.getString("CHECK_BANK_NAME"));
                        warrant.setCheckCusName(rs.getString("CHECK_CUS_NAME"));
                        warrant.setCheckAccountName(rs.getString("CHECK_ACCOUNT_NAME"));
                        warrant.setCheckAccountNumber(rs
                                                            .getString("CHECK_ACCOUNT_NUMBER"));
                        warrant.setCheckCusId(rs.getString("CHECK_CUS_ID"));
                        warrant.setCheckSignName(rs.getString("CHECK_SIGN_NAME"));
                        warrant.setCheckSignDate(rs.getString("CHECK_SIGN_DATE"));
                        warrant.setCheckLogo(rs.getString("CHECK_LOGO"));
                        warrant.setCheckResult(rs.getString("CHECK_RESULT"));
                  }
                  return warrant;
            } catch (Exception e) {
                  throw new Exception("��ѯ������Ȩ��ʶ��/�ȶԼ�¼,SQLִ���쳣" + e.getMessage(), e);
            } finally {
                  try {
                        closeDB();
                  } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              /**
        * ��ѯ������Ȩ��OCR�ȶԼ�¼
        *
        * @throws Exception
        */
              public Warrant selectWarrantByImageId(String imageId) throws SQLException {
            Warrant warrant = null;
            try {
                  String sql = "select * from warrant where image_id = ?";
                  Object[] objs = { imageId };
                  openIDS();
                  executeQuery(sql, objs);
                  if (rs.next()) {
                        warrant = new Warrant();
                        warrant.setImageId(rs.getString("IMAGE_ID"));
                        warrant.setApplicationId(rs.getString("APPLICATION_ID"));
                        warrant.setCreateDate(rs.getTimestamp("CREATE_DATE"));
                        warrant.setBankName(rs.getString("BANK_NAME"));
                        warrant.setCusName(rs.getString("CUS_NAME"));
                        warrant.setAccountName(rs.getString("ACCOUNT_NAME"));
                        warrant.setAccountNumber(rs.getString("ACCOUNT_NUMBER"));
                        warrant.setCusId(rs.getString("CUS_ID"));
                        System.out.println("get cusId: " + rs.getString("CUS_ID"));
                        warrant.setSignName(rs.getString("SIGN_NAME"));
                        warrant.setSignDate(rs.getString("SIGN_DATE"));
                        warrant.setLogo(rs.getString("LOGO"));
                        warrant.setTimeCost(rs.getInt("TIME_COST"));
                        warrant.setErrorCode(rs.getInt("ERROR_CODE"));
                        warrant.setErrorMsg(rs.getString("ERROR_MSG"));
                        warrant.setCheckBankName(rs.getString("CHECK_BANK_NAME"));
                        warrant.setCheckCusName(rs.getString("CHECK_CUS_NAME"));
                        warrant.setCheckAccountName(rs.getString("CHECK_ACCOUNT_NAME"));
                        warrant.setCheckAccountNumber(rs
                                                            .getString("CHECK_ACCOUNT_NUMBER"));
                        warrant.setCheckCusId(rs.getString("CHECK_CUS_ID"));
                        warrant.setCheckSignName(rs.getString("CHECK_SIGN_NAME"));
                        warrant.setCheckSignDate(rs.getString("CHECK_SIGN_DATE"));
                        warrant.setCheckLogo(rs.getString("CHECK_LOGO"));
                        warrant.setCheckResult(rs.getString("CHECK_RESULT"));
                  }
                  return warrant;
            } catch (SQLException e) {
                  throw new SQLException("��ѯ������Ȩ��ʶ��/�ȶԼ�¼,SQLִ���쳣" + e.getMessage(), e);
            } finally {
                  try {
                        closeDB();
                  } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                  }
            }
 
      }
 
              /**
        * ���»�����Ȩ��OCR�ȶԼ�¼(ȫ����)
        *
        * @throws Exception
        */
              public int updateWarrant(Warrant warrant) throws SQLException {
            try {
                  String sql = "UPDATE  warrant  SET application_id=?,create_date=?,bank_name=?,cus_name=?,account_name=?,account_number=?,cus_id=?,sign_name=?,sign_date=?,logo=?,time_cost=?,error_code=?,error_msg=?,check_bank_name=?,check_cus_name=?,check_account_name=?,check_account_number=?,check_cus_id=?,check_sign_name=?,check_sign_date=?,check_logo=?,check_result=? where image_id = ?";
                  Object[] objs = { warrant.getApplicationId(),
                              warrant.getCreateDate(), warrant.getBankName(),
                              warrant.getCusName(), warrant.getAccountName(),
                              warrant.getAccountNumber(), warrant.getCusId(),
                              warrant.getSignName(), warrant.getSignDate(),
                              warrant.getLogo(), warrant.getTimeCost(),
                              warrant.getErrorCode(), warrant.getErrorMsg(),
                              warrant.getCheckBankName(), warrant.getCheckCusName(),
                              warrant.getCheckAccountName(),
                              warrant.getCheckAccountNumber(), warrant.getCheckCusId(),
                              warrant.getCheckSignName(), warrant.getCheckSignDate(),
                              warrant.getCheckLogo(), warrant.getCheckResult(),
                              warrant.getImageId() };
                  openIDS();
                  return executeUpdate(sql, objs);
            } catch (Exception e) {
                  throw new SQLException("���»�����Ȩ��OCR�ȶԼ�¼ִ��SQL�����쳣:" + e.getMessage(),
                                                  e);
            } finally {
                  try {
                        closeDB();
                  } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              /**
        * ������Ѻ�����ͬOCRʶ��/�ȶԼ�¼
        */
              public int inserCompact(Compact compact) throws SQLException {
            try {
                  String sql = "INSERT INTO COMPACT (IMAGE_ID,APPLICATION_ID,CREATE_DATE,BUSI_NO,BORROWER_NAME,BORROWER_ID,CO_BORROWER_NAME,CO_BORROWER_ID,GAURANTOR_NAME,GAURANTOR_ID,DEALER_NAME,BRAND,LOAN_AMOUNT,LOAN_AMOUNT_NUMBER,DEBX_PERIOD,DEBJ_PERIOD,TXD_PERIOD,TXD_LEFT,JGCP_PERIOD,CONTRACT_RATE,ACTUAL_RATE,SUBSIDY_RATE,CONTRACT_TERM,CONTRACT_PRICE,VIN_NBR,BORROWER_SIGN,CO_BORROWER_SIGN,GAURANTOR_SIGN,WITNESS_SIGN,WITNESS_SIGN_DATE,LOGO,TIME_COST,ERROR_CODE,ERROR_MSG,CHECK_BUSI_NO,CHECK_BORROWER_NAME,CHECK_BORROWER_ID,CHECK_CO_BORROWER_NAME,CHECK_CO_BORROWER_ID,CHECK_GAURANTOR_NAME,CHECK_GAURANTOR_ID,CHECK_DEALER_NAME,CHECK_BRAND,CHECK_LOAN_AMOUNT,CHECK_LOAN_AMOUNT_NUMBER,CHECK_DEBX_PERIOD,CHECK_DEBJ_PERIOD,CHECK_TXD_PERIOD,CHECK_TXD_LEFT,CHECK_JGCP_PERIOD,CHECK_CONTRACT_RATE,CHECK_ACTUAL_RATE,CHECK_SUBSIDY_RATE,CHECK_CONTRACT_TERM,CHECK_CONTRACT_PRICE,CHECK_VIN_NBR,CHECK_BORROWER_SIGN,CHECK_CO_BORROWER_SIGN,CHECK_GAURANTOR_SIGN,CHECK_WITNESS_SIGN,CHECK_WITNESS_SIGN_DATE,CHECK_LOGO,CHECK_RESULT,CAR_TOTAL_PRICE,ATTACHMENT_TOTAL_PRICE,ATTACHMENT_LOAN_AMOUT_BIG,ATTACHMENT_LOAN_AMOUT_NUMBER,CAR_PURCHESE_TAX,CAR_INSTALLATION_FEE,INSURANCE_FEE,MAINTENANCE_FEE,EXTENDED_WARRANTY_SERVICE_FEE,CAR_USAGE,DEALER_ID,CHECK_CAR_TOTAL_PRICE,CHECK_ATTACHMENT_TOTAL_PRICE,CHECK_LOAN_AMOUT_BIG,CHECK_LOAN_AMOUT_NUMBER,CHECK_CAR_PURCHESE_TAX,CHECK_CAR_INSTALLATION_FEE,CHECK_INSURANCE_FEE,CHECK_MAINTENANCE_FEE,CHECK_EXTENDED_WARRANTYFEE,CHECK_CAR_USAGE,CHECK_DEALER_ID) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
                  Object[] objs = { compact.getImageId(), compact.getApplicationId(),
                              compact.getCreateDate(), compact.getBusiNo(),
                              compact.getBorrowerName(), compact.getBorrowerId(),
                              compact.getCoBorrowerName(), compact.getCoBorrowerId(),
                              compact.getGaurantorName(), compact.getGaurantorId(),
                              compact.getDealerName(), compact.getBrand(),
                              compact.getLoanAmount(), compact.getLoanAmountNumber(),
                              compact.getDebxPeriod(), compact.getDebjPeriod(),
                              compact.getTxdPeriod(), compact.getTxdLeft(),
                              compact.getJgcpPeriod(), compact.getContractRate(),
                              compact.getActualRate(), compact.getSubsidyRate(),
                              compact.getContractTerm(), compact.getContractPrice(),
                              compact.getVinNbr(), compact.getBorrowerSign(),
                              compact.getCoBorrowerSign(), compact.getGaurantorSign(),
                              compact.getWitnessSign(), compact.getWitnessSignDate(),
                              compact.getLogo(), compact.getTimeCost(),
                              compact.getErrorCode(), compact.getErrorMsg(),
                              compact.getCheckBusiNo(), compact.getCheckBorrowerName(),
                              compact.getCheckBorrowerId(),
                              compact.getCheckCoBorrowerName(),
                              compact.getCheckCoBorrowerId(),
                              compact.getCheckGaurantorName(),
                              compact.getCheckGaurantorId(),
                              compact.getCheckDealerName(), compact.getCheckBrand(),
                              compact.getCheckLoanAmount(),
                              compact.getCheckLoanAmountNumber(),
                              compact.getCheckDebxPeriod(), compact.getCheckDebjPeriod(),
                              compact.getCheckTxdPeriod(), compact.getCheckTxdLeft(),
                              compact.getCheckJgcpPeriod(),
                              compact.getCheckContractRate(),
                              compact.getCheckActualRate(),
                              compact.getCheckSubsidyRate(),
                              compact.getCheckContractTerm(),
                              compact.getCheckContractRate(), compact.getCheckVinNbr(),
                              compact.getCheckBorrowerSign(),
                              compact.getCheckCoBorrowerSign(),
                              compact.getCheckGaurantorSign(),
                              compact.getCheckWitnessSign(),
                              compact.getCheckWitnessSignDate(), compact.getCheckLogo(),
                              compact.getCheckResult() ,compact.getCarTotalPrice(),
                              compact.getAttachmentTotalPrice(),compact.getAttachmentLoanAmoutBig(),
                              compact.getAttachmentLoanAmoutNumber(),compact.getCarPurcheseTax(),
                              compact.getCarInstallationFee(),compact.getInsuranceFee(),
                              compact.getMaintenanceFee(),compact.getExtendedWarrantyServiceFee(),
                              compact.getCarUsage(),compact.getDealerId(),
                              compact.getCheckCarTotalPrice(),compact.getCheckAttachmentTotalPrice(),
                              compact.getCheckAttachmentLoanAmoutBig(),compact.getCheckAttachmentLoanAmoutNumber(),
                              compact.getCheckCarPurcheseTax(),compact.getCheckCarInstallationFee(),
                              compact.getCheckInsuranceFee(),compact.getCheckMaintenanceFee(),
                              compact.getCheckExtendedWarrantyServiceFee(),compact.getCheckCarUsage(),
                              compact.getCheckDealerId()};
                  if (isCompactExit(compact.getImageId())) {
                        return updateCompact(compact);
                  } else {
                        openIDS();
                        return executeUpdate(sql, objs);
                  }
            } catch (Exception e) {
                  throw new SQLException("������Ѻ�����ͬOCRʶ��/�ȶԼ�¼SQLִ���쳣��"
                                                  + e.getMessage(), e);
            } finally {
                  try {
                        closeDB();
                  } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              /**
        * ��Ѻ�����ͬOCRʶ��ȶԼ�¼�Ƿ����
        *
        * @param imageId
        * @return
        * @throws Exception
        */
              public boolean isCompactExit(String imageId) throws SQLException {
            boolean isExist = false;
            try {
                  String sql = "select count(*) is_exist from compact where image_id=?";
                  Object[] objs = { imageId };
                  openIDS();
                  executeQuery(sql, objs);
                  if (rs.next()) {
                        isExist = !"0".equals(rs.getString("is_exist"));
                  }
                  return isExist;
            } catch (SQLException e) {
                  throw e;
            } finally {
                  try {
                        closeDB();
                  } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              /**
        * ��ѯ��Ѻ�����ͬOCRʶ��ȶԼ�¼
        */
              public Compact selectCompactByApplicationId(String applicationId)
                  throws SQLException {
            try {
                  String sql = "select * from(select * from compact where application_id = ? order by create_date desc) where rownum=1";
                  Object[] objs = { applicationId };
                  openIDS();
                  executeQuery(sql, objs);
                  Compact compact = null;
                  if (rs.next()) {
                        compact = new Compact();
                        compact.setImageId(rs.getString("IMAGE_ID"));
                        compact.setApplicationId(rs.getString("APPLICATION_ID"));
                        compact.setCreateDate(rs.getTimestamp("CREATE_DATE"));
                        compact.setBusiNo(rs.getString("BUSI_NO"));
                        compact.setBorrowerName(rs.getString("BORROWER_NAME"));
                        compact.setBorrowerId(rs.getString("BORROWER_ID"));
                        compact.setCoBorrowerName(rs.getString("CO_BORROWER_NAME"));
                        compact.setCoBorrowerId(rs.getString("CO_BORROWER_ID"));
                        compact.setGaurantorName(rs.getString("GAURANTOR_NAME"));
                        compact.setGaurantorId(rs.getString("GAURANTOR_ID"));
                        compact.setDealerName(rs.getString("DEALER_NAME"));
                        compact.setBrand(rs.getString("BRAND"));
                        compact.setLoanAmount(rs.getString("LOAN_AMOUNT"));
                        compact.setLoanAmountNumber(rs.getString("LOAN_AMOUNT_NUMBER"));
                        compact.setDebxPeriod(rs.getString("DEBX_PERIOD"));
                        compact.setDebjPeriod(rs.getString("DEBJ_PERIOD"));
                        compact.setTxdPeriod(rs.getString("TXD_PERIOD"));
                        compact.setTxdLeft(rs.getString("TXD_LEFT"));
                        compact.setJgcpPeriod(rs.getString("JGCP_PERIOD"));
                        compact.setContractRate(rs.getString("CONTRACT_RATE"));
                        compact.setActualRate(rs.getString("ACTUAL_RATE"));
                        compact.setSubsidyRate(rs.getString("SUBSIDY_RATE"));
                        compact.setContractTerm(rs.getString("CONTRACT_TERM"));
                        compact.setContractPrice(rs.getString("CONTRACT_PRICE"));
                        compact.setVinNbr(rs.getString("VIN_NBR"));
                        compact.setBorrowerSign(rs.getString("BORROWER_SIGN"));
                        compact.setCoBorrowerSign(rs.getString("CO_BORROWER_SIGN"));
                        compact.setGaurantorSign(rs.getString("GAURANTOR_SIGN"));
                        compact.setWitnessSign(rs.getString("WITNESS_SIGN"));
                        compact.setWitnessSignDate(rs.getString("WITNESS_SIGN_DATE"));
                        compact.setLogo(rs.getString("LOGO"));
                        compact.setTimeCost(rs.getInt("TIME_COST"));
                        compact.setErrorCode(rs.getInt("ERROR_CODE"));
                        compact.setErrorMsg(rs.getString("ERROR_MSG"));
                        compact.setCheckBusiNo(rs.getString("CHECK_BUSI_NO"));
                        compact.setCheckBorrowerName(rs
                                                            .getString("CHECK_BORROWER_NAME"));
                        compact.setCheckBorrowerId(rs.getString("CHECK_BORROWER_ID"));
                        compact.setCheckCoBorrowerName(rs
                                                            .getString("CHECK_CO_BORROWER_NAME"));
                        compact.setCheckCoBorrowerId(rs
                                                            .getString("CHECK_CO_BORROWER_ID"));
                        compact.setCheckGaurantorName(rs
                                                            .getString("CHECK_GAURANTOR_NAME"));
                        compact.setCheckGaurantorId(rs.getString("CHECK_GAURANTOR_ID"));
                        compact.setCheckDealerName(rs.getString("CHECK_DEALER_NAME"));
                        compact.setCheckBrand(rs.getString("CHECK_BRAND"));
                        compact.setCheckLoanAmount(rs.getString("CHECK_LOAN_AMOUNT"));
                        compact.setCheckLoanAmountNumber(rs
                                                            .getString("CHECK_LOAN_AMOUNT_NUMBER"));
                        compact.setCheckDebxPeriod(rs.getString("CHECK_DEBX_PERIOD"));
                        compact.setCheckDebjPeriod(rs.getString("CHECK_DEBJ_PERIOD"));
                        compact.setCheckTxdPeriod(rs.getString("CHECK_TXD_PERIOD"));
                        compact.setCheckTxdLeft(rs.getString("CHECK_TXD_LEFT"));
                        compact.setCheckJgcpPeriod(rs.getString("CHECK_JGCP_PERIOD"));
                        compact.setCheckContractRate(rs
                                                            .getString("CHECK_CONTRACT_RATE"));
                        compact.setCheckActualRate(rs.getString("CHECK_ACTUAL_RATE"));
                        compact.setCheckSubsidyRate(rs.getString("CHECK_SUBSIDY_RATE"));
                        compact.setCheckContractTerm(rs
                                                            .getString("CHECK_CONTRACT_TERM"));
                        compact.setCheckContractPrice(rs
                                                            .getString("CHECK_CONTRACT_PRICE"));
                        compact.setCheckVinNbr(rs.getString("CHECK_VIN_NBR"));
                        compact.setCheckBorrowerSign(rs
                                                            .getString("CHECK_BORROWER_SIGN"));
                        compact.setCheckCoBorrowerSign(rs
                                                            .getString("CHECK_CO_BORROWER_SIGN"));
                        compact.setCheckGaurantorSign(rs
                                                            .getString("CHECK_GAURANTOR_SIGN"));
                        compact.setCheckWitnessSign(rs.getString("CHECK_WITNESS_SIGN"));
                        compact.setCheckWitnessSignDate(rs
                                                            .getString("CHECK_WITNESS_SIGN_DATE"));
                        compact.setCheckLogo(rs.getString("CHECK_LOGO"));
                        compact.setCheckResult(rs.getString("CHECK_RESULT"));
                        compact.setCarTotalPrice(rs.getString("CAR_TOTAL_PRICE"));
                        compact.setAttachmentTotalPrice(rs.getString("ATTACHMENT_TOTAL_PRICE"));
                        compact.setAttachmentLoanAmoutBig(rs.getString("ATTACHMENT_LOAN_AMOUT_BIG"));
                        compact.setAttachmentLoanAmoutNumber(rs.getString("ATTACHMENT_LOAN_AMOUT_NUMBER"));
                        compact.setCarPurcheseTax(rs.getString("CAR_PURCHESE_TAX"));
                        compact.setCarInstallationFee(rs.getString("CAR_INSTALLATION_FEE"));
                        compact.setInsuranceFee(rs.getString("INSURANCE_FEE"));
                        compact.setMaintenanceFee(rs.getString("MAINTENANCE_FEE"));
                        compact.setExtendedWarrantyServiceFee(rs.getString("EXTENDED_WARRANTY_SERVICE_FEE"));
                        compact.setCarUsage(rs.getString("CAR_USAGE"));
                        compact.setDealerId(rs.getString("DEALER_ID"));
                        compact.setCheckCarTotalPrice(rs.getString("CHECK_CAR_TOTAL_PRICE"));
                        compact.setCheckAttachmentTotalPrice(rs.getString("CHECK_ATTACHMENT_TOTAL_PRICE"));
                        compact.setCheckAttachmentLoanAmoutBig(rs.getString("CHECK_LOAN_AMOUT_BIG"));
                        compact.setCheckAttachmentLoanAmoutNumber(rs.getString("CHECK_LOAN_AMOUT_NUMBER"));
                        compact.setCheckCarPurcheseTax(rs.getString("CHECK_CAR_PURCHESE_TAX"));
                        compact.setCheckCarInstallationFee(rs.getString("CHECK_CAR_INSTALLATION_FEE"));
                        compact.setCheckInsuranceFee(rs.getString("CHECK_INSURANCE_FEE"));
                        compact.setCheckMaintenanceFee(rs.getString("CHECK_MAINTENANCE_FEE"));
                        compact.setCheckExtendedWarrantyServiceFee(rs.getString("CHECK_EXTENDED_WARRANTYFEE"));
                        compact.setCheckCarUsage(rs.getString("CHECK_CAR_USAGE"));
                        compact.setCheckDealerId(rs.getString("CHECK_DEALER_ID"));
                  }
                  return compact;
            } catch (Exception e) {
                  throw new SQLException(
                                                  "��ѯ��Ѻ�����ͬOCRʶ��ȶԼ�¼SQLִ���쳣:" + e.getMessage(), e);
            } finally {
                  try {
                        closeDB();
                  } catch (Exception e) {
                        logger.error(e.getMessage());
                  }
            }
      }
 
              /**
        * ��ѯ��Ѻ�����ͬocrʶ��ȶԼ�¼
        */
              public Compact selectCompactByImageId(String imageId) throws Exception {
            try {
                  String sql = "select * from(select * from compact where image_id = ?) where rownum=1";
                  Object[] objs = { imageId };
                  openIDS();
                  executeQuery(sql, objs);
                  Compact compact = null;
                  if (rs.next()) {
                        compact = new Compact();
                        compact.setImageId(rs.getString("IMAGE_ID"));
                        compact.setApplicationId(rs.getString("APPLICATION_ID"));
                        compact.setCreateDate(rs.getTimestamp("CREATE_DATE"));
                        compact.setBusiNo(rs.getString("BUSI_NO"));
                        compact.setBorrowerName(rs.getString("BORROWER_NAME"));
                        compact.setBorrowerId(rs.getString("BORROWER_ID"));
                        compact.setCoBorrowerName(rs.getString("CO_BORROWER_NAME"));
                        compact.setCoBorrowerId(rs.getString("CO_BORROWER_ID"));
                        compact.setGaurantorName(rs.getString("GAURANTOR_NAME"));
                        compact.setGaurantorId(rs.getString("GAURANTOR_ID"));
                        compact.setDealerName(rs.getString("DEALER_NAME"));
                        compact.setBrand(rs.getString("BRAND"));
                        compact.setLoanAmount(rs.getString("LOAN_AMOUNT"));
                        compact.setLoanAmountNumber(rs.getString("LOAN_AMOUNT_NUMBER"));
                        compact.setDebxPeriod(rs.getString("DEBX_PERIOD"));
                        compact.setDebjPeriod(rs.getString("DEBJ_PERIOD"));
                        compact.setTxdPeriod(rs.getString("TXD_PERIOD"));
                        compact.setTxdLeft(rs.getString("TXD_LEFT"));
                        compact.setJgcpPeriod(rs.getString("JGCP_PERIOD"));
                        compact.setContractRate(rs.getString("CONTRACT_RATE"));
                        compact.setActualRate(rs.getString("ACTUAL_RATE"));
                        compact.setSubsidyRate(rs.getString("SUBSIDY_RATE"));
                        compact.setContractTerm(rs.getString("CONTRACT_TERM"));
                        compact.setContractPrice(rs.getString("CONTRACT_PRICE"));
                        compact.setVinNbr(rs.getString("VIN_NBR"));
                        compact.setBorrowerSign(rs.getString("BORROWER_SIGN"));
                        compact.setCoBorrowerSign(rs.getString("CO_BORROWER_SIGN"));
                        compact.setGaurantorSign(rs.getString("GAURANTOR_SIGN"));
                        compact.setWitnessSign(rs.getString("WITNESS_SIGN"));
                        compact.setWitnessSignDate(rs.getString("WITNESS_SIGN_DATE"));
                        compact.setLogo(rs.getString("LOGO"));
                        compact.setTimeCost(rs.getInt("TIME_COST"));
                        compact.setErrorCode(rs.getInt("ERROR_CODE"));
                        compact.setErrorMsg(rs.getString("ERROR_MSG"));
                        compact.setCheckBusiNo(rs.getString("CHECK_BUSI_NO"));
                        compact.setCheckBorrowerName(rs
                                                            .getString("CHECK_BORROWER_NAME"));
                        compact.setCheckBorrowerId(rs.getString("CHECK_BORROWER_ID"));
                        compact.setCheckCoBorrowerName(rs
                                                            .getString("CHECK_CO_BORROWER_NAME"));
                        compact.setCheckCoBorrowerId(rs
                                                            .getString("CHECK_CO_BORROWER_ID"));
                        compact.setCheckGaurantorName(rs
                                                            .getString("CHECK_GAURANTOR_NAME"));
                        compact.setCheckGaurantorId(rs.getString("CHECK_GAURANTOR_ID"));
                        compact.setCheckDealerName(rs.getString("CHECK_DEALER_NAME"));
                        compact.setCheckBrand(rs.getString("CHECK_BRAND"));
                        compact.setCheckLoanAmount(rs.getString("CHECK_LOAN_AMOUNT"));
                        compact.setCheckLoanAmountNumber(rs
                                                            .getString("CHECK_LOAN_AMOUNT_NUMBER"));
                        compact.setCheckDebxPeriod(rs.getString("CHECK_DEBX_PERIOD"));
                        compact.setCheckDebjPeriod(rs.getString("CHECK_DEBJ_PERIOD"));
                        compact.setCheckTxdPeriod(rs.getString("CHECK_TXD_PERIOD"));
                        compact.setCheckTxdLeft(rs.getString("CHECK_TXD_LEFT"));
                        compact.setCheckJgcpPeriod(rs.getString("CHECK_JGCP_PERIOD"));
                        compact.setCheckContractRate(rs
                                                            .getString("CHECK_CONTRACT_RATE"));
                        compact.setCheckActualRate(rs.getString("CHECK_ACTUAL_RATE"));
                        compact.setCheckSubsidyRate(rs.getString("CHECK_SUBSIDY_RATE"));
                        compact.setCheckContractTerm(rs
                                                            .getString("CHECK_CONTRACT_TERM"));
                        compact.setCheckContractPrice(rs
                                                            .getString("CHECK_CONTRACT_PRICE"));
                        compact.setCheckVinNbr(rs.getString("CHECK_VIN_NBR"));
                        compact.setCheckBorrowerSign(rs
                                                            .getString("CHECK_BORROWER_SIGN"));
                        compact.setCheckCoBorrowerSign(rs
                                                            .getString("CHECK_CO_BORROWER_SIGN"));
                        compact.setCheckGaurantorSign(rs
                                                            .getString("CHECK_GAURANTOR_SIGN"));
                        compact.setCheckWitnessSign(rs.getString("CHECK_WITNESS_SIGN"));
                        compact.setCheckWitnessSignDate(rs
                                                            .getString("CHECK_WITNESS_SIGN_DATE"));
                        compact.setCheckLogo(rs.getString("CHECK_LOGO"));
                        compact.setCheckResult(rs.getString("CHECK_RESULT"));
                        compact.setCarTotalPrice(rs.getString("CAR_TOTAL_PRICE"));
                        compact.setAttachmentTotalPrice(rs.getString("ATTACHMENT_TOTAL_PRICE"));
                        compact.setAttachmentLoanAmoutBig(rs.getString("ATTACHMENT_LOAN_AMOUT_BIG"));
                        compact.setAttachmentLoanAmoutNumber(rs.getString("ATTACHMENT_LOAN_AMOUT_NUMBER"));
                        compact.setCarPurcheseTax(rs.getString("CAR_PURCHESE_TAX"));
                        compact.setCarInstallationFee(rs.getString("CAR_INSTALLATION_FEE"));
                        compact.setInsuranceFee(rs.getString("INSURANCE_FEE"));
                        compact.setMaintenanceFee(rs.getString("MAINTENANCE_FEE"));
                        compact.setExtendedWarrantyServiceFee(rs.getString("EXTENDED_WARRANTY_SERVICE_FEE"));
                        compact.setCarUsage(rs.getString("CAR_USAGE"));
                        compact.setDealerId(rs.getString("DEALER_ID"));
                        compact.setCheckCarTotalPrice(rs.getString("CHECK_CAR_TOTAL_PRICE"));
                        compact.setCheckAttachmentTotalPrice(rs.getString("CHECK_ATTACHMENT_TOTAL_PRICE"));
                        compact.setCheckAttachmentLoanAmoutBig(rs.getString("CHECK_LOAN_AMOUT_BIG"));
                        compact.setCheckAttachmentLoanAmoutNumber(rs.getString("CHECK_LOAN_AMOUT_NUMBER"));
                        compact.setCheckCarPurcheseTax(rs.getString("CHECK_CAR_PURCHESE_TAX"));
                        compact.setCheckCarInstallationFee(rs.getString("CHECK_CAR_INSTALLATION_FEE"));
                        compact.setCheckInsuranceFee(rs.getString("CHECK_INSURANCE_FEE"));
                        compact.setCheckMaintenanceFee(rs.getString("CHECK_MAINTENANCE_FEE"));
                        compact.setCheckExtendedWarrantyServiceFee(rs.getString("CHECK_EXTENDED_WARRANTYFEE"));
                        compact.setCheckCarUsage(rs.getString("CHECK_CAR_USAGE"));
                        compact.setCheckDealerId(rs.getString("CHECK_DEALER_ID"));
                  }
                  return compact;
            } catch (Exception e) {
                  throw new SQLException(
                                                  "��ѯ��Ѻ�����ͬOCRʶ��ȶԼ�¼SQLִ���쳣:" + e.getMessage(), e);
            } finally {
                  try {
                        closeDB();
                  } catch (Exception e) {
                        logger.error(e.getMessage());
                  }
            }
      }
 
              /**
        * ���µ�Ѻ�����ͬOCRʶ��ȶԼ�¼
        */
              public int updateCompact(Compact compact) throws SQLException {
            try {
                  String sql = "UPDATE COMPACT SET APPLICATION_ID = ?,CREATE_DATE = ?,BUSI_NO = ?,BORROWER_NAME = ?,BORROWER_ID = ?,CO_BORROWER_NAME = ?,CO_BORROWER_ID = ?,GAURANTOR_NAME = ?,GAURANTOR_ID = ?,DEALER_NAME = ?,BRAND = ?,LOAN_AMOUNT = ?,LOAN_AMOUNT_NUMBER = ?,DEBX_PERIOD = ?,DEBJ_PERIOD = ?,TXD_PERIOD = ?,TXD_LEFT = ?,JGCP_PERIOD = ?,CONTRACT_RATE = ?,ACTUAL_RATE = ?,SUBSIDY_RATE = ?,CONTRACT_TERM = ?,CONTRACT_PRICE = ?,VIN_NBR = ?,BORROWER_SIGN = ?,CO_BORROWER_SIGN = ?,GAURANTOR_SIGN = ?,WITNESS_SIGN = ?,WITNESS_SIGN_DATE = ?,LOGO = ?,TIME_COST = ?,ERROR_CODE = ?,ERROR_MSG = ?,CHECK_BUSI_NO = ?,CHECK_BORROWER_NAME = ?,CHECK_BORROWER_ID = ?,CHECK_CO_BORROWER_NAME = ?,CHECK_CO_BORROWER_ID = ?,CHECK_GAURANTOR_NAME = ?,CHECK_GAURANTOR_ID = ?,CHECK_DEALER_NAME = ?,CHECK_BRAND = ?,CHECK_LOAN_AMOUNT = ?,CHECK_LOAN_AMOUNT_NUMBER = ?,CHECK_DEBX_PERIOD = ?,CHECK_DEBJ_PERIOD = ?,CHECK_TXD_PERIOD = ?,CHECK_TXD_LEFT = ?,CHECK_JGCP_PERIOD = ?,CHECK_CONTRACT_RATE = ?,CHECK_ACTUAL_RATE = ?,CHECK_SUBSIDY_RATE = ?,CHECK_CONTRACT_TERM = ?,CHECK_CONTRACT_PRICE = ?,CHECK_VIN_NBR = ?,CHECK_BORROWER_SIGN = ?,CHECK_CO_BORROWER_SIGN = ?,CHECK_GAURANTOR_SIGN = ?,CHECK_WITNESS_SIGN = ?,CHECK_WITNESS_SIGN_DATE = ?,CHECK_LOGO = ?,CHECK_RESULT = ?,CAR_TOTAL_PRICE=?,ATTACHMENT_TOTAL_PRICE=?,ATTACHMENT_LOAN_AMOUT_BIG=?,ATTACHMENT_LOAN_AMOUT_NUMBER=?,CAR_PURCHESE_TAX=?,CAR_INSTALLATION_FEE=?,INSURANCE_FEE=?,MAINTENANCE_FEE=?,EXTENDED_WARRANTY_SERVICE_FEE=?,CAR_USAGE=?,DEALER_ID=?,CHECK_CAR_TOTAL_PRICE=?,CHECK_ATTACHMENT_TOTAL_PRICE=?,CHECK_LOAN_AMOUT_BIG=?,CHECK_LOAN_AMOUT_NUMBER=?,CHECK_CAR_PURCHESE_TAX=?,CHECK_CAR_INSTALLATION_FEE=?,CHECK_INSURANCE_FEE=?,CHECK_MAINTENANCE_FEE=?,CHECK_EXTENDED_WARRANTYFEE=?,CHECK_CAR_USAGE=?,CHECK_DEALER_ID=? where IMAGE_ID = ?";
                  Object[] objs = { compact.getApplicationId(),
                              compact.getCreateDate(), compact.getBusiNo(),
                              compact.getBorrowerName(), compact.getBorrowerId(),
                              compact.getCoBorrowerName(), compact.getCoBorrowerId(),
                              compact.getGaurantorName(), compact.getGaurantorId(),
                              compact.getDealerName(), compact.getBrand(),
                              compact.getLoanAmount(), compact.getLoanAmountNumber(),
                              compact.getDebxPeriod(), compact.getDebjPeriod(),
                              compact.getTxdPeriod(), compact.getTxdLeft(),
                              compact.getJgcpPeriod(), compact.getContractRate(),
                              compact.getActualRate(), compact.getSubsidyRate(),
                              compact.getContractTerm(), compact.getContractPrice(),
                              compact.getVinNbr(), compact.getBorrowerSign(),
                              compact.getCoBorrowerSign(), compact.getGaurantorSign(),
                              compact.getWitnessSign(), compact.getWitnessSignDate(),
                              compact.getLogo(), compact.getTimeCost(),
                              compact.getErrorCode(), compact.getErrorMsg(),
                              compact.getCheckBusiNo(), compact.getCheckBorrowerName(),
                              compact.getCheckBorrowerId(),
                              compact.getCheckCoBorrowerName(),
                              compact.getCheckCoBorrowerName(),
                              compact.getCheckGaurantorName(),
                              compact.getCheckGaurantorId(),
                              compact.getCheckDealerName(), compact.getCheckBrand(),
                              compact.getCheckLoanAmount(),
                              compact.getCheckLoanAmountNumber(),
                              compact.getCheckDebxPeriod(), compact.getCheckDebjPeriod(),
                              compact.getCheckTxdPeriod(), compact.getCheckTxdLeft(),
                              compact.getCheckJgcpPeriod(),
                              compact.getCheckContractRate(),
                              compact.getCheckActualRate(),
                              compact.getCheckSubsidyRate(),
                              compact.getCheckContractTerm(),
                              compact.getCheckContractPrice(), compact.getCheckVinNbr(),
                              compact.getCheckBorrowerSign(),
                              compact.getCheckCoBorrowerSign(),
                              compact.getCheckGaurantorSign(),
                              compact.getCheckWitnessSign(),
                              compact.getCheckWitnessSignDate(), compact.getCheckLogo(),
                              compact.getCheckResult(),
                              compact.getCarTotalPrice(),
                              compact.getAttachmentTotalPrice(),compact.getAttachmentLoanAmoutBig(),
                              compact.getAttachmentLoanAmoutNumber(),compact.getCarPurcheseTax(),
                              compact.getCarInstallationFee(),compact.getInsuranceFee(),
                              compact.getMaintenanceFee(),compact.getExtendedWarrantyServiceFee(),
                              compact.getCarUsage(),compact.getDealerId(),
                              compact.getCheckCarTotalPrice(),compact.getCheckAttachmentTotalPrice(),
                              compact.getCheckAttachmentLoanAmoutBig(),compact.getCheckAttachmentLoanAmoutNumber(),
                              compact.getCheckCarPurcheseTax(),compact.getCheckCarInstallationFee(),
                              compact.getCheckInsuranceFee(),compact.getCheckMaintenanceFee(),
                              compact.getCheckExtendedWarrantyServiceFee(),compact.getCheckCarUsage(),
                              compact.getCheckDealerId(),compact.getImageId() };
                  openIDS();
                  return executeUpdate(sql, objs);
            } catch (SQLException e) {
                  throw new SQLException("���µ�Ѻ�����ͬOCRʶ��/�ȶԼ�¼SQLִ���쳣��"
                                                  + e.getMessage(), e);
            } finally {
                  try {
                        closeDB();
                  } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              /**
        * ��ȡ��Ѻ�����ͬ���ȶ��ֶ�
        */
              public Compact selectCompactCheck(String applicationId) throws Exception {
            try {
                  String sql = "select * from(select image_id,application_id,create_date,busi_no,borrower_name,borrower_id,co_borrower_name,co_borrower_id,gaurantor_name,gaurantor_id,dealer_name,brand,loan_amount,loan_amount_number,debx_period,debj_period,txd_period,txd_left,jgcp_period,contract_rate,actual_rate,subsidy_rate,contract_term,contract_price,vin_nbr,borrower_sign,co_borrower_sign,gaurantor_sign,witness_sign,witness_sign_date,logo,time_cost,error_code,error_msg,car_total_price,attachment_total_price,attachment_loan_amout_big,attachment_loan_amout_number,car_purchese_tax,car_installation_fee,insurance_fee,maintenance_fee,extended_warranty_service_fee,car_usage,dealer_id from compact where application_id = ? order by create_date desc) where rownum=1";
                  Object[] objs = { applicationId };
                  openIDS();
                  executeQuery(sql, objs);
                  Compact compact = null;
                  if (rs.next()) {
                        compact = new Compact();
                        compact.setImageId(rs.getString("IMAGE_ID"));
                        compact.setApplicationId(rs.getString("APPLICATION_ID"));
                        compact.setCreateDate(rs.getTimestamp("CREATE_DATE"));
                        compact.setBusiNo(rs.getString("BUSI_NO"));
                        compact.setBorrowerName(rs.getString("BORROWER_NAME"));
                        compact.setBorrowerId(rs.getString("BORROWER_ID"));
                        compact.setCoBorrowerName(rs.getString("CO_BORROWER_NAME"));
                        compact.setCoBorrowerId(rs.getString("CO_BORROWER_ID"));
                        compact.setGaurantorName(rs.getString("GAURANTOR_NAME"));
                        compact.setGaurantorId(rs.getString("GAURANTOR_ID"));
                        compact.setDealerName(rs.getString("DEALER_NAME"));
                        compact.setBrand(rs.getString("BRAND"));
                        compact.setLoanAmount(rs.getString("LOAN_AMOUNT"));
                        compact.setLoanAmountNumber(rs.getString("LOAN_AMOUNT_NUMBER"));
                        compact.setDebxPeriod(rs.getString("DEBX_PERIOD"));
                        compact.setDebjPeriod(rs.getString("DEBJ_PERIOD"));
                        compact.setTxdPeriod(rs.getString("TXD_PERIOD"));
                        compact.setTxdLeft(rs.getString("TXD_LEFT"));
                        compact.setJgcpPeriod(rs.getString("JGCP_PERIOD"));
                        compact.setContractRate(rs.getString("CONTRACT_RATE"));
                        compact.setActualRate(rs.getString("ACTUAL_RATE"));
                        compact.setSubsidyRate(rs.getString("SUBSIDY_RATE"));
                        compact.setContractTerm(rs.getString("CONTRACT_TERM"));
                        compact.setContractPrice(rs.getString("CONTRACT_PRICE"));
                        compact.setVinNbr(rs.getString("VIN_NBR"));
                        compact.setBorrowerSign(rs.getString("BORROWER_SIGN"));
                        compact.setCoBorrowerSign(rs.getString("CO_BORROWER_SIGN"));
                        compact.setGaurantorSign(rs.getString("GAURANTOR_SIGN"));
                        compact.setWitnessSign(rs.getString("WITNESS_SIGN"));
                        compact.setWitnessSignDate(rs.getString("WITNESS_SIGN_DATE"));
                        compact.setLogo(rs.getString("LOGO"));
                        compact.setTimeCost(rs.getInt("TIME_COST"));
                        compact.setErrorCode(rs.getInt("ERROR_CODE"));
                        compact.setErrorMsg(rs.getString("ERROR_MSG"));
                        compact.setCarTotalPrice(rs.getString("CAR_TOTAL_PRICE"));
                        compact.setAttachmentTotalPrice(rs.getString("ATTACHMENT_TOTAL_PRICE"));
                        compact.setAttachmentLoanAmoutBig(rs.getString("ATTACHMENT_LOAN_AMOUT_BIG"));
                        compact.setAttachmentLoanAmoutNumber(rs.getString("ATTACHMENT_LOAN_AMOUT_NUMBER"));
                        compact.setCarPurcheseTax(rs.getString("CAR_PURCHESE_TAX"));
                        compact.setCarInstallationFee(rs.getString("CAR_INSTALLATION_FEE"));
                        compact.setInsuranceFee(rs.getString("INSURANCE_FEE"));
                        compact.setMaintenanceFee(rs.getString("MAINTENANCE_FEE"));
                        compact.setExtendedWarrantyServiceFee(rs.getString("EXTENDED_WARRANTY_SERVICE_FEE"));
                        compact.setCarUsage(rs.getString("CAR_USAGE"));
                        compact.setDealerId(rs.getString("DEALER_ID"));
                  }
                  return compact;
            } catch (Exception e) {
                  throw new SQLException(
                                                  "��ѯ��Ѻ�����ͬOCRʶ��ȶԼ�¼SQLִ���쳣:" + e.getMessage(), e);
            } finally {
                  try {
                        closeDB();
                  } catch (Exception e) {
                        logger.error(e.getMessage());
                  }
            }
      }
 
              /**
        * ��ȡ������Ʊ���ȶ��ֶ�
        */
              public VehicleInvoice selectInvoiceCheck(String applicationId)
                  throws Exception {
            VehicleInvoice invoice = null;
            try {
                  String sql = "select * from(select image_id,application_id,create_date,invoice_date,invoice_no,borrower_name,borrower_id,engine_no,vin_no,amount,amount_number,dealer_name,time_cost,error_code,error_msg from vehicle_invoice where application_id = ? order by create_date DESC)where rownum = 1";
                  Object[] objs = { applicationId };
                  openIDS();
                  executeQuery(sql, objs);
                  if (rs.next()) {
                        invoice = new VehicleInvoice();
                        invoice.setImageId(rs.getString("image_id"));
                        invoice.setApplicationId(rs.getString("application_id"));
                        invoice.setCreateDate(rs.getTimestamp("create_date"));
                        invoice.setImageId(rs.getString("image_id"));
                        invoice.setInvoiceDate(rs.getString("invoice_date"));
                        invoice.setInvoiceNo(rs.getString("invoice_no"));
                        invoice.setBorrowerName(rs.getString("borrower_name"));
                        invoice.setBorrowerId(rs.getString("borrower_id"));
                        invoice.setEngineNo(rs.getString("engine_no"));
                        invoice.setVinNo(rs.getString("vin_no"));
                        invoice.setAmount(rs.getString("amount"));
                        invoice.setAmountNumber(rs.getString("amount_number"));
                        invoice.setDealerName(rs.getString("dealer_name"));
                        invoice.setCostTime(rs.getInt("time_cost"));
                        invoice.setErrorCode(rs.getInt("error_code"));
                        invoice.setErrorMsg(rs.getString("error_msg"));
                  }
            } catch (SQLException e) {
                  throw new SQLException("��ѯ������Ʊʶ��/�ȶԼ�¼,SQLִ���쳣" + e.getMessage(), e);
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
            return invoice;
      }
 
              /**
        * ��ȡ������Ȩ����ȶ��ֶ�
        */
              public Warrant selectWarrantCheck(String applicationId) throws Exception {
            Warrant warrant = null;
            try {
                  String sql = "select * from(select image_id,application_id,create_date,bank_name,cus_name,account_name,account_number,cus_id,sign_name,sign_date,logo,time_cost,error_code,error_msg from warrant where application_id = ? order by create_date desc)where rownum = 1";
                  Object[] objs = { applicationId };
                  openIDS();
                  executeQuery(sql, objs);
                  if (rs.next()) {
                        warrant = new Warrant();
                        warrant.setImageId(rs.getString("IMAGE_ID"));
                        warrant.setApplicationId(rs.getString("APPLICATION_ID"));
                        warrant.setCreateDate(rs.getTimestamp("CREATE_DATE"));
                        warrant.setBankName(rs.getString("BANK_NAME"));
                        warrant.setCusName(rs.getString("CUS_NAME"));
                        warrant.setAccountName(rs.getString("ACCOUNT_NAME"));
                        warrant.setAccountNumber(rs.getString("ACCOUNT_NUMBER"));
                        warrant.setCusId(rs.getString("CUS_ID"));
                        warrant.setSignName(rs.getString("SIGN_NAME"));
                        warrant.setSignDate(rs.getString("SIGN_DATE"));
                        warrant.setLogo(rs.getString("LOGO"));
                        warrant.setTimeCost(rs.getInt("TIME_COST"));
                        warrant.setErrorCode(rs.getInt("ERROR_CODE"));
                        warrant.setErrorMsg(rs.getString("ERROR_MSG"));
                  }
                  return warrant;
            } catch (Exception e) {
                  throw new Exception("��ѯ������Ȩ��ʶ��/�ȶԼ�¼,SQLִ���쳣" + e.getMessage(), e);
            } finally {
                  try {
                        closeDB();
                  } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              /**
        * ���µ�Ѻ�����ͬ�ȶԽ��
        */
              public int updateCompactCheck(Compact compact) throws Exception {
            try {
                  String sql = "UPDATE COMPACT SET CHECK_BUSI_NO = ?,CHECK_BORROWER_NAME = ?,CHECK_BORROWER_ID = ?,CHECK_CO_BORROWER_NAME = ?,CHECK_CO_BORROWER_ID = ?,CHECK_GAURANTOR_NAME = ?,CHECK_GAURANTOR_ID = ?,CHECK_DEALER_NAME = ?,CHECK_BRAND = ?,CHECK_LOAN_AMOUNT = ?,CHECK_LOAN_AMOUNT_NUMBER = ?,CHECK_DEBX_PERIOD = ?,CHECK_DEBJ_PERIOD = ?,CHECK_TXD_PERIOD = ?,CHECK_TXD_LEFT = ?,CHECK_JGCP_PERIOD = ?,CHECK_CONTRACT_RATE = ?,CHECK_ACTUAL_RATE = ?,CHECK_SUBSIDY_RATE = ?,CHECK_CONTRACT_TERM = ?,CHECK_CONTRACT_PRICE = ?,CHECK_VIN_NBR = ?,CHECK_BORROWER_SIGN = ?,CHECK_CO_BORROWER_SIGN = ?,CHECK_GAURANTOR_SIGN = ?,CHECK_WITNESS_SIGN = ?,CHECK_WITNESS_SIGN_DATE = ?,CHECK_LOGO = ?,CHECK_RESULT = ?,CHECK_CAR_TOTAL_PRICE = ?,CHECK_ATTACHMENT_TOTAL_PRICE = ?,CHECK_LOAN_AMOUT_BIG = ?,CHECK_LOAN_AMOUT_NUMBER = ?,CHECK_CAR_PURCHESE_TAX = ?,CHECK_CAR_INSTALLATION_FEE = ?,CHECK_INSURANCE_FEE = ?,CHECK_MAINTENANCE_FEE = ?,CHECK_EXTENDED_WARRANTYFEE = ?,CHECK_CAR_USAGE = ?,CHECK_DEALER_ID = ? where IMAGE_ID = ?";
                  Object[] objs = { compact.getCheckBusiNo(),
                              compact.getCheckBorrowerName(),
                              compact.getCheckBorrowerId(),
                              compact.getCheckCoBorrowerName(),
                              compact.getCheckCoBorrowerName(),
                              compact.getCheckGaurantorName(),
                              compact.getCheckGaurantorId(),
                              compact.getCheckDealerName(), compact.getCheckBrand(),
                              compact.getCheckLoanAmount(),
                              compact.getCheckLoanAmountNumber(),
                              compact.getCheckDebxPeriod(), compact.getCheckDebjPeriod(),
                              compact.getCheckTxdPeriod(), compact.getCheckTxdLeft(),
                              compact.getCheckJgcpPeriod(),
                              compact.getCheckContractRate(),
                              compact.getCheckActualRate(),
                              compact.getCheckSubsidyRate(),
                              compact.getCheckContractTerm(),
                              compact.getCheckContractPrice(), compact.getCheckVinNbr(),
                              compact.getCheckBorrowerSign(),
                              compact.getCheckCoBorrowerSign(),
                              compact.getCheckGaurantorSign(),
                              compact.getCheckWitnessSign(),
                              compact.getCheckWitnessSignDate(), compact.getCheckLogo(),
                              compact.getCheckResult(),
                              compact.getCheckCarTotalPrice(),compact.getCheckAttachmentTotalPrice(),
                              compact.getCheckAttachmentLoanAmoutBig(),compact.getCheckAttachmentLoanAmoutNumber(),
                              compact.getCheckCarPurcheseTax(),compact.getCheckCarInstallationFee(),
                              compact.getCheckInsuranceFee(),compact.getCheckMaintenanceFee(),
                              compact.getCheckExtendedWarrantyServiceFee(),compact.getCheckCarUsage(),
                              compact.getCheckDealerId(),compact.getImageId() };
                  openIDS();
                  return executeUpdate(sql, objs);
            } catch (SQLException e) {
                  throw new SQLException("���µ�Ѻ�����ͬ�ȶԽ��SQLִ���쳣��" + e.getMessage(), e);
            } finally {
                  try {
                        closeDB();
                  } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              /**
        * ���¹�����Ʊ�ȶԽ��
        */
              public int updateInvoiceCheck(VehicleInvoice invoice) throws Exception {
            try {
                  String sql = "UPDATE vehicle_invoice SET check_invoice_date=?,check_borrower_name=?,check_borrower_id=?,check_vin_no=?,check_amount=?,check_amount_number=?,check_dealer_name=?,check_result=?"
                              + " where image_id=?";
                  Object[] objs = { invoice.getCheckInvoiceDate(),
                              invoice.getCheckBorrowerName(),
                              invoice.getCheckBorrowerId(), invoice.getCheckVinNo(),
                              invoice.getCheckAmount(), invoice.getCheckAmountNumber(),
                              invoice.getCheckDealerName(), invoice.getCheckResult(),
                              invoice.getImageId() };
                  openIDS();
                  return executeUpdate(sql, objs);
            } catch (SQLException e) {
                  throw new SQLException("���¹�����Ʊʶ��/�ȶԼ�¼ִ��SQL�����쳣: " + e.getMessage(),
                                                  e);
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              /**
        * ���»�����Ȩ��ȶԽ��
        */
              public int updateWarrantCheck(Warrant warrant) throws Exception {
            try {
                  String sql = "UPDATE  warrant  SET check_bank_name=?,check_cus_name=?,check_account_name=?,check_account_number=?,check_cus_id=?,check_sign_name=?,check_sign_date=?,check_logo=?,check_result=? where image_id = ?";
                  Object[] objs = { warrant.getCheckBankName(),
                              warrant.getCheckCusName(), warrant.getCheckAccountName(),
                              warrant.getCheckAccountNumber(), warrant.getCheckCusId(),
                              warrant.getCheckSignName(), warrant.getCheckSignDate(),
                              warrant.getCheckLogo(), warrant.getCheckResult(),
                              warrant.getImageId() };
                  openIDS();
                  return executeUpdate(sql, objs);
            } catch (Exception e) {
                  throw new SQLException("���»�����Ȩ��OCR�ȶԼ�¼ִ��SQL�����쳣:" + e.getMessage(),
                                                  e);
            } finally {
                  try {
                        closeDB();
                  } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              /**
        * ��ȡCMS�еıȶ��ֶ�
        *
        * @throws Exception
        */
              public Map<String, String> selectCheckItem(String applicationId)
                  throws SQLException {
            Map<String, String> result = null;
            try {
                  //String sql = "select * from ocr_check_item_new join (select * from(select a.loan_id,a.task_end_datim,a.pri_type,a.flag,b.application_no from task a join (select application_no,loan_id from loan where application_no=?)b on a.loan_id=b.loan_id where a.pri_type='1' and a.flag='8' order by a.task_end_datim)where rownum=1)c on ocr_check_item_new.application_number = c.application_no";
                  String sql ="select * from(select a.loan_id,a.task_end_datim,a.pri_type,a.flag,b.application_no from task a join (select application_no,loan_id from loan where application_no=?)b on a.loan_id=b.loan_id where a.pri_type='1' and a.flag='8' order by a.task_end_datim)where rownum=1";
                  String sql1= "select *  from ocr_check_item_new where application_number=?";
                  Object[] objs = { applicationId };
                  openSDS();
                  executeQuery(sql, objs);
                  rsmd = rs.getMetaData();
                  if (rs.next()) {
                        result = new HashMap<String, String>();
                        for (int i = 0; i < rsmd.getColumnCount(); i++) {
                              result.put(rsmd.getColumnName(i + 1), rs.getString(i + 1));
                        }
                  }
                  executeQuery(sql1, objs);
                  rsmd = rs.getMetaData();
                  if (rs.next()) {
                        for (int i = 0; i < rsmd.getColumnCount(); i++) {
                              result.put(rsmd.getColumnName(i + 1), rs.getString(i + 1));
                        }
                  }
                  return result;
            } catch (SQLException e) {
                  throw new SQLException("��ȡCMS�еıȶ��ֶ�,SQLִ���쳣:" + e.getMessage(), e);
            } finally {
                  try {
                        closeDB();
                  } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              /**
        * ��ȡCMS���뵥��Ϣ
        */
              public Map<String, String> selectLoanInfo(String applicationId)
                  throws SQLException {
            try {
                  Map<String, String> result = null;
                  String sql = "select * from ocr_check_item_new where application_number = ?";
                  Object[] objs = { applicationId };
                  openSDS();
                  executeQuery(sql, objs);
                  rsmd = rs.getMetaData();
                  if (rs.next()) {
                        result = new HashMap<String, String>();
                        for (int i = 0; i < rsmd.getColumnCount(); i++) {
                              result.put(rsmd.getColumnName(i + 1), rs.getString(i + 1));
                        }
                  }
                  return result;
            } catch (SQLException e) {
                  throw e;
            } finally {
                  try {
                        closeDB();
                  } catch (Exception e) {
                        logger.error(e.getMessage());
                  }
            }
      }
 
              /**
        * ��ȡ���뵥��������
        */
              public Map<String, String> selectTaskEndTime(String applicationId)
                  throws SQLException {
            try {
                  Map<String, String> result = null;
                  String sql = "select * from(select a.loan_id,a.task_end_datim,a.pri_type,a.flag,b.application_no from task a join (select application_no,loan_id from loan where application_no=?)b on a.loan_id=b.loan_id where a.pri_type='1' and a.flag='8' order by a.task_end_datim)where rownum=1";
                  Object[] objs = { applicationId };
                  openSDS();
                  executeQuery(sql, objs);
                  rsmd = rs.getMetaData();
                  if (rs.next()) {
                        result = new HashMap<String, String>();
                        for (int i = 0; i < rsmd.getColumnCount(); i++) {
                              result.put(rsmd.getColumnName(i + 1), rs.getString(i + 1));
                        }
                  }
                  return result;
            } catch (SQLException e) {
                  throw e;
            } finally {
                  try {
                        closeDB();
                  } catch (Exception e) {
                        logger.error(e.getMessage());
                  }
            }
      }
      /**
        * �������п�OCR�ȶԼ�¼
        *
        * @throws Exception
        */
              public int inserBankcard(BankCard bc) throws Exception {
            String sql = "insert into bank_card (IMAGE_ID, APPLICATION_ID, CREATE_DATE, CARD_NUMBER, ISSUER, TYPE, TIME_COST, ERROR_CODE, ERROR_MSG, CHECK_BANK_NAME, CHECK_ACCOUNT_NUMBER, CHECK_RESULT) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?)";
            Object[] objs = { bc.getImageId(), bc.getApplicationId(),
                        bc.getCreateDate(),bc.getCardNumber(),bc.getIssuer(),
                        bc.getType(),bc.getCostTime(),bc.getErrorCode(),
                        bc.getErrorMsg(),bc.getCheckBankName(),bc.getCheckAccountNumber(),
                        bc.getCheckResult()};
            try {
                  if (isBankCardExsit(bc)) {
                        return updateBankCard(bc);
                  } else {
                        openIDS();
                        return executeUpdate(sql, objs);
                  }
            } catch (SQLException e) {
                  throw new SQLException("�������п�OCR�ȶԼ�¼SQLִ���쳣�� " + e.getMessage(),
                                                  e);
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              /**
        * �������п�OCR�ȶԼ�¼(ȫ����)
        *
        * @throws Exception
        */
              public int updateBankCard(BankCard bc) throws SQLException {
            try {
                  String sql = "UPDATE  bank_card  SET application_id=?,CREATE_DATE=?, CARD_NUMBER=?, ISSUER=?, TYPE=?, TIME_COST=?, ERROR_CODE=?, ERROR_MSG=?, CHECK_BANK_NAME=?, CHECK_ACCOUNT_NUMBER=?, CHECK_RESULT=? where image_id = ?";
                  Object[] objs = { bc.getApplicationId(),
                              bc.getCreateDate(),bc.getCardNumber(),bc.getIssuer(),
                              bc.getType(),bc.getCostTime(),bc.getErrorCode(),
                              bc.getErrorMsg(),bc.getCheckBankName(),bc.getCheckAccountNumber(),
                              bc.getCheckResult()};
                  openIDS();
                  return executeUpdate(sql, objs);
            } catch (Exception e) {
                  throw new SQLException("�������п�OCR�ȶԼ�¼ִ��SQL�����쳣:" + e.getMessage(),
                                                  e);
            } finally {
                  try {
                        closeDB();
                  } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              public BankCard selectBankcardCheck(String applicationId) throws Exception {
            BankCard bc = null;
            try {
                  String sql = "select * from(select image_id,application_id,create_date,card_number,issuer,type,time_cost,error_code,error_msg,check_bank_name,check_account_number,check_result from bank_card where application_id = ? order by create_date desc)where rownum = 1";
                  Object[] objs = { applicationId };
                  openIDS();
                  executeQuery(sql, objs);
                  if (rs.next()) {
                        bc = new BankCard();
                        bc.setImageId(rs.getString("IMAGE_ID"));
                        bc.setApplicationId(rs.getString("APPLICATION_ID"));
                        bc.setCreateDate(rs.getTimestamp("CREATE_DATE"));
                        bc.setCardNumber(rs.getString("CARD_NUMBER"));
                        bc.setIssuer(rs.getString("ISSUER"));
                        bc.setType(rs.getString("TYPE"));
                        bc.setCostTime(rs.getInt("TIME_COST"));
                        bc.setErrorCode(rs.getInt("ERROR_CODE"));
                        bc.setErrorMsg(rs.getString("ERROR_MSG"));
                        bc.setCheckBankName(rs.getString("CHECK_BANK_NAME"));
                        bc.setCheckAccountNumber(rs.getString("CHECK_ACCOUNT_NUMBER"));
                        bc.setCheckResult(rs.getString("CHECK_RESULT"));
                  }
                  return bc;
            } catch (Exception e) {
                  throw new Exception("��ѯ���п�ʶ��/�ȶԼ�¼,SQLִ���쳣" + e.getMessage(), e);
            } finally {
                  try {
                        closeDB();
                  } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
      /**
        * �������п��ȶԽ��
        */
              public int updateBankcardCheck(BankCard bankcard) throws Exception {
            try {
                  String sql = "UPDATE  bank_card  SET CHECK_BANK_NAME=?,CHECK_ACCOUNT_NUMBER=?,CHECK_RESULT=? where image_id = ?";
                  Object[] objs = { bankcard.getCheckBankName(),
                              bankcard.getCheckAccountNumber(),
                              bankcard.getCheckResult(),
                              bankcard.getImageId() };
                  openIDS();
                  return executeUpdate(sql, objs);
            } catch (Exception e) {
                  throw new SQLException("�������п�OCR�ȶԼ�¼ִ��SQL�����쳣:" + e.getMessage(),
                                                  e);
            } finally {
                  try {
                        closeDB();
                  } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
      /**
        * ��ѯ���п�ʶ��/�ȶԼ�¼
        */
              public BankCard selectBankcardByImageId(String imageId) throws Exception {
            BankCard bc = null;
            try {
                  String sql = "select * from bank_card where image_id = ?";
                  Object[] objs = { imageId };
                  openIDS();
                  executeQuery(sql, objs);
                  if (rs.next()) {
                        bc = new BankCard();
                        bc.setImageId(rs.getString("IMAGE_ID"));
                        bc.setApplicationId(rs.getString("APPLICATION_ID"));
                        bc.setCreateDate(rs.getTimestamp("CREATE_DATE"));
                        bc.setCardNumber(rs.getString("CARD_NUMBER"));
                        bc.setIssuer(rs.getString("ISSUER"));
                        bc.setType(rs.getString("TYPE"));
                        bc.setCostTime(rs.getInt("TIME_COST"));
                        bc.setErrorCode(rs.getInt("ERROR_CODE"));
                        bc.setErrorMsg(rs.getString("ERROR_MSG"));
                        bc.setCheckBankName(rs.getString("CHECK_BANK_NAME"));
                        bc.setCheckAccountNumber(rs.getString("CHECK_ACCOUNT_NUMBER"));
                        bc.setCheckResult(rs.getString("CHECK_RESULT"));
                  }
                  return bc;
            } catch (SQLException e) {
                  throw new SQLException("��ѯ���п�ʶ��/�ȶԼ�¼,SQLִ���쳣" + e.getMessage(), e);
            } finally {
                  try {
                        closeDB();
                  } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
      public int inserCardId(CardId cid) throws Exception {
            String sql = "insert into card_id (IMAGE_ID, APPLICATION_ID, CREATE_DATE, ID_NAME, ID_NUMBER, VALIDITY,IAMGE_TYPE,TIME_COST, ERROR_CODE, ERROR_MSG, CHECK_ID_NAME, CHECK_ID_NUMBER, CHECK_RESULT,PEOPLE) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?)";
            Object[] objs = { cid.getImageId(), cid.getApplicationId(),
                        cid.getCreateDate(),cid.getIdName(),cid.getId_number(),cid.getValidity(),cid.getImage_type()
                        ,cid.getCostTime(),cid.getErrorCode(),cid.getErrorMsg()
                        ,cid.getCheckIdName(),cid.getCheckIdNumber(),cid.getCheckResult(),cid.getPeople()};
            try {
                  if (isCardIdExist(cid.getImageId())) {
                        return updateCardIdCheck(cid);
                  } else {
                        openIDS();
                        return executeUpdate(sql, objs);
                  }
            } catch (SQLException e) {
                  throw new SQLException("�������֤��ϢOCR�ȶԼ�¼SQLִ���쳣�� " + e.getMessage(),
                                                  e);
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
      /**
        * ��ʻ֤OCRʶ��ȶԼ�¼�Ƿ����
        *
        * @param imageId
        * @return
        * @throws Exception
        */
              public boolean isCardIdExist(String imageId) throws SQLException {
            boolean isExist = false;
            try {
                  String sql = "select count(*) is_exist from card_id where image_id=?";
                  Object[] objs = { imageId };
                  openIDS();
                  executeQuery(sql, objs);
                  if (rs.next()) {
                        isExist = !"0".equals(rs.getString("is_exist"));
                  }
                  return isExist;
            } catch (SQLException e) {
                  throw e;
            } finally {
                  try {
                        closeDB();
                  } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              public int updateCardIdCheck(CardId cid) throws Exception {
            try {
                  String sql = "UPDATE  card_id  SET CHECK_ID_NAME=?, CHECK_ID_NUMBER=?, CHECK_RESULT=? where image_id = ?";
                  Object[] objs = { cid.getCheckIdName(),cid.getCheckIdNumber(),
                              cid.getCheckResult(),
                              cid.getImageId() };
                  openIDS();
                  return executeUpdate(sql, objs);
            } catch (Exception e) {
                  throw new SQLException("�������֤��ϢOCR�ȶԼ�¼ִ��SQL�����쳣:" + e.getMessage(),
                                                  e);
            } finally {
                  try {
                        closeDB();
                  } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              public List<CardIdVw> selectCheckCardIdItem(String applicationId) throws Exception {
            List<CardIdVw> vwList=new ArrayList<CardIdVw>();
            try {
                  String sql = "select SOS_IDENTIFICATION_CODE,SOS_NAME,SOS_ID_CARD_TYPE,SOS_ID_CARD_NBR from APPLICANT_DETAIL_VW where sos_application_nbr=?";
                  Object[] objs = { applicationId };
                  openIDS();
                  executeQuery(sql, objs);
                  while (rs.next()) {
                        CardIdVw vw=new CardIdVw();
                        vw.setSos_application_nbr(applicationId);
                        vw.setSos_identification_code(rs.getString("SOS_IDENTIFICATION_CODE"));
                        vw.setSos_name(rs.getString("SOS_NAME"));
                        vw.setSos_id_card_type(rs.getString("SOS_ID_CARD_TYPE"));
                        vw.setSos_id_card_nbr(rs.getString("SOS_ID_CARD_NBR"));
                        vwList.add(vw);
                  }
                  return vwList;
            } catch (Exception e) {
                  throw new Exception("��ѯ���֤¼����Ϣ,SQLִ���쳣" + e.getMessage(), e);
            } finally {
                  try {
                        closeDB();
                  } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              public List<CardId> selectCardIdCheck(String applicationId) throws Exception {
            List<CardId> ocrList=new ArrayList<CardId>();
            try {
                  String sql = "select IMAGE_ID,CREATE_DATE, ID_NAME, ID_NUMBER, VALIDITY,IAMGE_TYPE,TIME_COST, ERROR_CODE, ERROR_MSG, CHECK_ID_NAME, CHECK_ID_NUMBER, CHECK_RESULT from CARD_ID where APPLICATION_ID=?";
                  Object[] objs = { applicationId };
                  openIDS();
                  executeQuery(sql, objs);
                  while (rs.next()) {
                        CardId vw=new CardId();
                        vw.setImageId(rs.getString("IMAGE_ID"));
                        vw.setIdName(rs.getString("ID_NAME"));
                        vw.setId_number(rs.getString("ID_NUMBER"));
                        vw.setValidity(rs.getString("VALIDITY"));
                        vw.setImage_type(rs.getString("IAMGE_TYPE"));
                        ocrList.add(vw);
                  }
                  return ocrList;
            } catch (Exception e) {
                  throw new Exception("��ѯ���֤ocr��Ϣ,SQLִ���쳣" + e.getMessage(), e);
            } finally {
                  try {
                        closeDB();
                  } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              public List<CardId> selectCardIdByType(String applicationId) throws Exception {
            List<CardId> ocrList=new ArrayList<CardId>();
            try {
                  String sql = "select IMAGE_ID,VALIDITY,IAMGE_TYPE,TIME_COST, ERROR_CODE, ERROR_MSG, CHECK_ID_NAME, CHECK_ID_NUMBER, CHECK_RESULT from CARD_ID where APPLICATION_ID=? and IAMGE_TYPE=?";
                  String type="�ڶ������֤����";
                  Object[] objs = { applicationId,type };
                  openIDS();
                  executeQuery(sql, objs);
                  while (rs.next()) {
                        CardId vw=new CardId();
                        vw.setImageId(rs.getString("IMAGE_ID"));
                        //vw.setIdName(rs.getString("ID_NAME"));
                        //vw.setId_number(rs.getString("ID_NUMBER"));
                        vw.setValidity(rs.getString("VALIDITY"));
                        vw.setImage_type(rs.getString("IAMGE_TYPE"));
                        ocrList.add(vw);
                  }
                  return ocrList;
            } catch (Exception e) {
                  throw new Exception("��ѯ���֤����ocr��Ϣ,SQLִ���쳣" + e.getMessage(), e);
            } finally {
                  try {
                        closeDB();
                  } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              public List<CardId> checkCardInfo(CardId queryInfo) throws Exception {
            List<CardId> ocrList=new ArrayList<CardId>();
            try {
                  String sql = "select IMAGE_ID,CREATE_DATE, ID_NAME, ID_NUMBER, VALIDITY,IAMGE_TYPE,TIME_COST, ERROR_CODE, ERROR_MSG, CHECK_ID_NAME, CHECK_ID_NUMBER, CHECK_RESULT from CARD_ID where APPLICATION_ID=? and ID_NAME=? and ID_NUMBER=?";
                  String type="�ڶ������֤����";
                  Object[] objs = { queryInfo.getApplicationId(),queryInfo.getIdName(),queryInfo.getId_number()};
                  openIDS();
                  executeQuery(sql, objs);
                  while (rs.next()) {
                        CardId vw=new CardId();
                        vw.setImageId(rs.getString("IMAGE_ID"));
                        vw.setIdName(rs.getString("ID_NAME"));
                        vw.setId_number(rs.getString("ID_NUMBER"));
                        //vw.setValidity(rs.getString("VALIDITY"));
                        vw.setImage_type(rs.getString("IAMGE_TYPE"));
                        ocrList.add(vw);
                  }
                  return ocrList;
            } catch (Exception e) {
                  throw new Exception("��ѯ���֤����ocr��Ϣ,SQLִ���쳣" + e.getMessage(), e);
            } finally {
                  try {
                        closeDB();
                  } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              public int updateCardIdComment(String loanId, String taskComment) throws Exception {
            String sql = null;
            String taskId=selectTaskId(loanId);
            String selSql = "select c.comment_id from comments c where c.comment_type='SI' and c.userid='20676' and c.loan_id=? and c.task_id=? and task_comment like '%���֤%' and c.display='1'";
            String upSql = "update comments set display='0' where comment_id=?";
            try {
                  sql= "INSERT INTO COMMENTS (COMMENT_ID,WORKITEM_ID,USERID,TASK_COMMENT,COMMENT_TYPE,COMMENT_DATIM,LOAN_ID,TASK_ID)" +
                              " values(?,'000102030405060000','20676',?,?,sysdate,?,?)";
                  Object[] objs = {loanId,taskId};
                  openIDS();
                  //��ѯ֮ǰ�����֤����
                  executeQuery(selSql, objs);
                  while (rs.next()) {
                        String commentId=rs.getString("comment_id");
                        Object[] objs1 = {commentId};
                        //��֮ǰ�����֤������Ϊ����ʾ
                        executeUpdate(upSql, objs1);
                  }
                  Object[] objs2 = { CommonFunc.getTraceNumber(),taskComment,"SI",loanId,taskId};
                  //�������µ����֤�ȶ�����
                  return executeUpdate(sql, objs2);
            } catch (SQLException e) {
                  throw new SQLException("�������֤�ȶ�����,SQLִ���쳣:" + e.getMessage(), e);
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              public int updateCardPbocComment(String loanId, String taskComment) throws Exception {
            String sql = null;
            String taskId=selectTaskId(loanId);
            String selSql = "select c.comment_id from comments c where c.comment_type='SI' and c.userid='20676' and c.loan_id=? and c.task_id=? and task_comment like '%��ϢУ��ȶ�%' and c.display='1'";
            String upSql = "update comments set display='0' where comment_id=?";
            try {
                  sql= "INSERT INTO COMMENTS (COMMENT_ID,WORKITEM_ID,USERID,TASK_COMMENT,COMMENT_TYPE,COMMENT_DATIM,LOAN_ID,TASK_ID)" +
                              " values(?,'000102030405060000','20676',?,?,sysdate,?,?)";
                  Object[] objs = {loanId,taskId};
                  openIDS();
                  //��ѯ֮ǰ�����֤����
                  executeQuery(selSql, objs);
                  while (rs.next()) {
                        String commentId=rs.getString("comment_id");
                        Object[] objs1 = {commentId};
                        //��֮ǰ�����֤������Ϊ����ʾ
                        executeUpdate(upSql, objs1);
                  }
                  Object[] objs2 = { CommonFunc.getTraceNumber(),taskComment,"SI",loanId,taskId};
                  //�������µ����֤�ȶ�����
                  return executeUpdate(sql, objs2);
            } catch (SQLException e) {
                  throw new SQLException("������ϢУ��ȶԱȶ�����,SQLִ���쳣:" + e.getMessage(), e);
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              public String selectTaskId(String loanId)throws Exception{
            String taskId="";
            try {
            String sql="select TASK_ID from task where PRI_TYPE='1' and LOAN_ID=?";
            Object[] objs = {loanId};
            openIDS();
            executeQuery(sql, objs);
            while (rs.next()) {
                  taskId=rs.getString("TASK_ID");
            }
            } catch (SQLException e) {
                  throw new SQLException("��ѯ�����ʶSQLִ���쳣:" + e.getMessage(), e);
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
            return taskId;
      }
 
              public InvoiceInfo selectInvoiceInfoByImageId(String imageId) throws Exception {
            InvoiceInfo invoice = null;
            try {
                  String sql = "select image_id,application_id,create_date,invoice_code,invoice_no,invoice_date,invoice_sum,amount,amount_number,borrower_name,borrower_id,vin_no,dealer_name,time_cost,error_code,error_msg from INVOICE_VERIFY_OCR where image_id = ?";
                  Object[] objs = { imageId };
                  openIDS();
                  executeQuery(sql, objs);
                  if (rs.next()) {
                        invoice = new InvoiceInfo();
                        invoice.setImageId(rs.getString("image_id"));
                        invoice.setApplicationId(rs.getString("application_id"));
                        invoice.setCreateDate(rs.getTimestamp("create_date"));
                        invoice.setInvoiceCode(rs.getString("invoice_code"));
                        invoice.setInvoiceNo(rs.getString("invoice_no"));
                        invoice.setInvoiceDate(rs.getString("invoice_date"));
                        invoice.setInvoiceSum(rs.getString("invoice_sum"));
                        invoice.setAmount(rs.getString("amount"));
                        invoice.setAmountNumber(rs.getString("amount_number"));
                        invoice.setBorrowerName(rs.getString("borrower_name"));
                        invoice.setBorrowerId(rs.getString("borrower_id"));
                        invoice.setVinNo(rs.getString("vin_no"));
                        invoice.setDealerName(rs.getString("dealer_name"));
                        invoice.setCostTime(rs.getInt("time_cost"));
                        invoice.setErrorCode(rs.getInt("error_code"));
                        invoice.setErrorMsg(rs.getString("error_msg"));
                  }
                  return invoice;
            } catch (SQLException e) {
                  throw new SQLException("��ѯ������Ʊʶ��/�ȶԼ�¼,SQLִ���쳣" + e.getMessage(), e);
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              public int insertInvoiceInfo(InvoiceInfo invoice) throws Exception {
            String sql = null;
            try {
                  sql = "insert into INVOICE_VERIFY_OCR (image_id,application_id,create_date,invoice_code,invoice_no,invoice_date,invoice_sum,amount,amount_number,borrower_name,borrower_id,vin_no,dealer_name,time_cost,error_code,error_msg) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
                  Object[] objs = { invoice.getImageId(), invoice.getApplicationId(),
                              invoice.getCreateDate(),invoice.getInvoiceCode(),
                              invoice.getInvoiceNo(), invoice.getInvoiceDate(),
                              invoice.getInvoiceSum(), invoice.getAmount(),
                              invoice.getAmountNumber(), invoice.getBorrowerName(),
                              invoice.getBorrowerId(),invoice.getVinNo(),
                              invoice.getDealerName(),invoice.getCostTime(),
                              invoice.getErrorCode(),invoice.getErrorMsg()};
                  if (isInvoiceInfoExist(invoice.getImageId())) {
                        return updateInvoiceInfo(invoice);
                  } else {
                        openIDS();
                        return executeUpdate(sql, objs);
                  }
            } catch (SQLException e) {
                  throw new SQLException("����������Ʊʶ��/�ȶԼ�¼,SQLִ���쳣:" + e.getMessage(), e);
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              public boolean isInvoiceInfoExist(String imageid) throws Exception {
            String sql = null;
            boolean isExist = false;
            try {
                  sql = "select count(*) is_exist from INVOICE_VERIFY_OCR where image_id =?";
                  Object[] objs = {imageid};
                  openIDS();
                  executeQuery(sql, objs);
                  if (rs.next()) {
                        isExist = !"0".equals(rs.getString("is_exist"));
                  }
                  return isExist;
            } catch (SQLException e) {
                  throw e;
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              public int updateInvoiceInfo(InvoiceInfo invoice) throws Exception {
            try {
                  String sql = "UPDATE INVOICE_VERIFY_OCR SET image_id=?,application_id=?,create_date=?,invoice_code=?,invoice_no=?,invoice_date=?,invoice_sum=?,amount=?,amount_number=?,borrower_name=?,borrower_id=?,vin_no=?,dealer_name=?,time_cost=?,error_code=?,error_msg=?"
                              + " where image_id=?";
                  Object[] objs = {invoice.getImageId(), invoice.getApplicationId(),
                              invoice.getCreateDate(),invoice.getInvoiceCode(),
                              invoice.getInvoiceNo(), invoice.getInvoiceDate(),
                              invoice.getInvoiceSum(), invoice.getAmount(),
                              invoice.getAmountNumber(), invoice.getBorrowerName(),
                              invoice.getBorrowerId(),invoice.getVinNo(),
                              invoice.getDealerName(),invoice.getCostTime(),
                              invoice.getErrorCode(),invoice.getErrorMsg()};
                  openIDS();
                  return executeUpdate(sql, objs);
            } catch (SQLException e) {
                  throw new SQLException("���¹�����Ʊʶ��/�ȶԼ�¼ִ��SQL�����쳣: " + e.getMessage(),
                                                  e);
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              public int insertVerifyInfo(VerifyInfo info)throws Exception{
            String sql = "insert into INVOICE_VERIFY_RESULT (RESULTID, APPLICATION_ID, SALER_NAME, PURCHASER_TAXPAYER_NUMBER, INVOICE_DATE, ALL_VALOREM_TAX, INVALID_MARK, ID_CARD, CARFRAME_CODE, PURCHASER_NAME, VERIFY_DATE, FIELD_FIRST,FIELD_SECOND,FIELD_THIRD,VERIFY_TYPE) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            Object[] objs = {CommonFunc.getTraceNumber(),info.getApplicationId(),
                        info.getSalerName(), info.getPurchasertaxpayerNumber(),
                        info.getInvoiceDate(),info.getAllvaloremTax(),
                        info.getInvalidMark(),info.getIdCard(),
                        info.getCarframeCode(),info.getPurchaserName(),
                        info.getVerifyDate(),info.getFieldFirst(),
                        info.getFieldSecond(),info.getFieldThird(),info.getVerifytype()};
            try {
                  openIDS();
                  return executeUpdate(sql, objs);
            } catch (SQLException e) {
                  throw new SQLException("������Ʊ�����¼SQLִ���쳣�� " + e.getMessage(),
                                                  e);
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              public boolean isVerifyInfoExist(String applicationId) throws Exception {
            String sql = null;
            boolean isExist = false;
            try {
                  sql = "select count(*) is_exist from INVOICE_VERIFY_RESULT where APPLICATION_ID =?";
                  Object[] objs = {applicationId};
                  openIDS();
                  executeQuery(sql, objs);
                  if (rs.next()) {
                        isExist = !"0".equals(rs.getString("is_exist"));
                  }
                  return isExist;
            } catch (SQLException e) {
                  throw e;
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              public int updateVerifyInfo(VerifyInfo invoice) throws Exception {
            try {
                  String sql = "UPDATE INVOICE_VERIFY_RESULT SET APPLICATION_ID=?,SALER_NAME=?,PURCHASER_TAXPAYER_NUMBER=?,INVOICE_DATE=?,ALL_VALOREM_TAX=?,INVALID_MARK=?,ID_CARD=?,CARFRAME_CODE=?,PURCHASER_NAME=?,VERIFY_DATE=?,FIELD_FIRST=?,FIELD_SECOND=?,FIELD_THIRD=?,VERIFY_TYPE=?"
                              + " where APPLICATION_ID=?";
                  Object[] objs = {invoice.getApplicationId(), invoice.getSalerName(),
                              invoice.getPurchasertaxpayerNumber(),invoice.getInvoiceDate(),
                              invoice.getAllvaloremTax(), invoice.getInvalidMark(),
                              invoice.getIdCard(), invoice.getCarframeCode(),
                              invoice.getPurchaserName(), invoice.getVerifyDate(),
                              invoice.getFieldFirst(),invoice.getFieldSecond(),
                              invoice.getFieldThird(),invoice.getVerifytype(),invoice.getApplicationId()};
                  openIDS();
                  return executeUpdate(sql, objs);
            } catch (SQLException e) {
                  throw new SQLException("���·�Ʊ������ִ��SQL�����쳣: " + e.getMessage(),
                                                  e);
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              public int againUpdateVerifyInfo(VerifyInfo info) throws Exception {
            try {
                  String sql = "UPDATE INVOICE_VERIFY_RESULT SET MANUAL_VERIFY=?,MANUAL_DATE=?"
                              + " where APPLICATION_ID=?";
                  Object[] objs = {info.getManualVerify(),info.getManualDate(),info.getApplicationId()};
                  openIDS();
                  return executeUpdate(sql, objs);
            } catch (SQLException e) {
                  throw new SQLException("���·�Ʊ������ִ��SQL�����쳣: " + e.getMessage(),
                                                  e);
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              public String selectVerifyInfo(String appno) throws Exception {
            String sql = null;
            String result="";
            try {
                  sql = "select MANUAL_VERIFY from INVOICE_VERIFY_RESULT where APPLICATION_ID =?";
                  Object[] objs = {appno};
                  openIDS();
                  executeQuery(sql, objs);
                  if (rs.next()) {
                        result = rs.getString("manual_verify");
                  }
                  return result;
            } catch (SQLException e) {
                  throw e;
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              public int insertBatchVerifyInfo(String appno,String verifytype,int userid,String content) throws Exception {
            String sql = "insert into BATCHINVOICE_VERIFY_RESULT (RESULTID, APPLICATION_ID,VERIFY_TYPE,USERID,VERIFY_STATUS,CONDITION_CONTENT) values (?,?,?,?,?,?)";
            Object[] objs = {CommonFunc.getTraceNumber(),appno,verifytype,userid,"0",content};
            try {
                  openIDS();
                  return executeUpdate(sql, objs);
            } catch (SQLException e) {
                  e.printStackTrace();
                  throw new SQLException("������Ʊ�����¼SQLִ���쳣 ",
                                                  e);
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
 
      }
 
              public int queryForInsert(String customerType, String dealerName, String starttime, String endtime,int userid,String content)
                  throws Exception {
            String querySql = "select l.application_no, l.isused, l.dealer_channel_flag from task t, loan l,dealer d,dealer_loan_map dlm  where t.pri_type = '2' and t.flag = '8' and t.task_status = '5'"+
                        " and t.loan_id = l.loan_id and l.cstmtype_id = '1' and (l.is_swc <> '1' or l.is_swc is null) and d.dealer_id=dlm.dealer_id and dlm.loan_id=l.loan_id"+
                        " and d.dealer_name=? and (TO_CHAR(t.task_end_datim, 'YYYYMMDD') >= ? OR ? IS NULL) and (TO_CHAR(t.task_end_datim, 'YYYYMMDD') <= ? OR ? IS NULL)";
            String querySql1="select l.application_no, l.isused, l.dealer_channel_flag from task t, loan l where t.pri_type = '2' and t.flag = '8' and t.task_status = '5' and t.loan_id = l.loan_id"+
                        " and l.cstmtype_id = '1' and (l.is_swc <> '1' or l.is_swc is null) and (TO_CHAR(t.task_end_datim, 'YYYYMMDD') >= ? OR ? IS NULL) and (TO_CHAR(t.task_end_datim, 'YYYYMMDD') <= ? OR ? IS NULL)";
            String sql = "insert into BATCHINVOICE_VERIFY_RESULT (RESULTID, APPLICATION_ID,VERIFY_TYPE,USERID,VERIFY_STATUS,CONDITION_CONTENT) values (?,?,?,?,?,?)";
            Object[] objs = {dealerName,starttime,starttime,endtime,endtime};
            Object[] objs1= {starttime,starttime,endtime,endtime};
            int count=0;
            try {
                  openIDS();
                  if("".equals(dealerName)) {
                        executeQuery(querySql1, objs1);
                  }else {
                        executeQuery(querySql, objs);
                  }
                  while (rs.next()) {
                        String appno = rs.getString("application_no");
                        Object[] obj = {CommonFunc.getTraceNumber(),appno,customerType,userid,"0",content};
                        executeUpdate(sql, obj);
                        count++;
                  }
                  return count;
            } catch (SQLException e) {
                  e.printStackTrace();
                  throw new SQLException("������Ʊ�����¼SQLִ���쳣",
                                                  e);
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              public int queryForLBrInsert(String appno, String verifytype, int userid,String content) throws Exception {
            String querySql = "select csv.APPLICATION_NUMBER from cl_fleet_summary_view csv where csv.CL_APPLICATION_NUMBER=? and csv.APPLICATION_FLOW='FLEET PROPOSAL'";
            String sql = "insert into BATCHINVOICE_VERIFY_RESULT (RESULTID, APPLICATION_ID,VERIFY_TYPE,USERID,VERIFY_STATUS,CONDITION_CONTENT) values (?,?,?,?,?,?)";
            int count=0;
            Object[] objs = {appno};
            try {
                  openIDS();
                  executeQuery(querySql, objs);
                  while (rs.next()) {
                        String appnumber = rs.getString("APPLICATION_NUMBER");
                        Object[] obj = {CommonFunc.getTraceNumber(),appnumber,verifytype,userid,"0",content};
                        executeUpdate(sql, obj);
                        count++;
                  }
                  return count;
            } catch (SQLException e) {
                  e.printStackTrace();
                  throw new SQLException("������Ʊ�����¼SQLִ���쳣 ",
                                                  e);
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              public int queryFor2Insert(String customerType, String dealerName, String starttime, String endtime, int userid,String content)
                  throws Exception {
            String querySql = "select l.application_no from task t, loan l ,dealer d,dealer_loan_map dlm where t.pri_type = '2' and t.flag = '8' and t.task_status = '5'" +
                        " and t.loan_id = l.loan_id and (l.cstmtype_id = '2' or l.is_swc = '1') and d.dealer_id=dlm.dealer_id and dlm.loan_id=l.loan_id"+
                        " and d.dealer_name=? and (TO_CHAR(t.task_end_datim, 'YYYYMMDD') >= ? OR ? IS NULL) and (TO_CHAR(t.task_end_datim, 'YYYYMMDD') <= ? OR ? IS NULL)";
            String querySql1="select l.application_no from task t, loan l where t.pri_type = '2' and t.flag = '8' and t.task_status = '5' and t.loan_id = l.loan_id and (l.cstmtype_id = '2' or l.is_swc = '1')"+
                        " and (TO_CHAR(t.task_end_datim, 'YYYYMMDD') >= ? OR ? IS NULL) and (TO_CHAR(t.task_end_datim, 'YYYYMMDD') <= ? OR ? IS NULL)";
            String sql = "insert into BATCHINVOICE_VERIFY_RESULT (RESULTID, APPLICATION_ID,VERIFY_TYPE,USERID,VERIFY_STATUS,CONDITION_CONTENT) values (?,?,?,?,?,?)";
            Object[] objs = {dealerName,starttime,starttime,endtime,endtime};
            Object[] objs1= {starttime,starttime,endtime,endtime};
            int count=0;
            try {
                  openIDS();
                  if("".equals(dealerName)) {
                        executeQuery(querySql1, objs1);
                  }else {
                        executeQuery(querySql, objs);
                  }
                  while (rs.next()) {
                        String appno = rs.getString("application_no");
                        Object[] obj = {CommonFunc.getTraceNumber(),appno,customerType,userid,"0",content};
                        executeUpdate(sql, obj);
                        count++;
                  }
                  return count;
            } catch (SQLException e) {
                  e.printStackTrace();
                  throw new SQLException("������Ʊ�����¼SQLִ���쳣 ",
                                                  e);
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              public int queryForBrMInsert(String customerType, String dealerName, String starttime, String endtime, int userid,String content)
                  throws Exception {
            String querySql = "select l.application_no from cl_fleet_summary_view csv, task t, loan l where t.pri_type = '2' and t.flag = '8' and t.task_status = '5'" +
                        " and csv.APPLICATION_FLOW = 'FLEET PROPOSAL' and csv.APPLICATION_NUMBER = l.application_no and t.loan_id = l.loan_id "+
                        " and csv.COMPANY_NAME=? and (TO_CHAR(t.task_end_datim, 'YYYYMMDD') >= ? OR ? IS NULL) and (TO_CHAR(t.task_end_datim, 'YYYYMMDD') <= ? OR ? IS NULL)";
            String sql = "insert into BATCHINVOICE_VERIFY_RESULT (RESULTID, APPLICATION_ID,VERIFY_TYPE,USERID,VERIFY_STATUS,CONDITION_CONTENT) values (?,?,?,?,?,?)";
            Object[] objs = {dealerName,starttime,starttime,endtime,endtime};
            int count=0;
            try {
                  openIDS();
                  executeQuery(querySql, objs);
                  while (rs.next()) {
                        String appno = rs.getString("application_no");
                        Object[] obj = {CommonFunc.getTraceNumber(),appno,customerType,userid,"0",content};
                        executeUpdate(sql, obj);
                        count++;
                  }
                  return count;
            } catch (SQLException e) {
                  e.printStackTrace();
                  throw new SQLException("������Ʊ�����¼SQLִ���쳣 ",
                                                  e);
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              public int updateBatchVerifyInfo(BatchVerifyInfo invoice) throws Exception {
            try {
                  String sql = "UPDATE BATCHINVOICE_VERIFY_RESULT SET SALER_NAME=?,PURCHASER_TAXPAYER_NUMBER=?,INVOICE_DATE=?,ALL_VALOREM_TAX=?,INVALID_MARK=?,ID_CARD=?,CARFRAME_CODE=?,PURCHASER_NAME=?,VERIFY_DATE=?,FIELD_FIRST=?,FIELD_SECOND=?,FIELD_THIRD=?,VERIFY_STATUS=?"
                              + " where RESULTID=?";
                  Object[] objs = {invoice.getSalerName(),
                              invoice.getPurchasertaxpayerNumber(),invoice.getInvoiceDate(),
                              invoice.getAllvaloremTax(), invoice.getInvalidMark(),
                              invoice.getIdCard(), invoice.getCarframeCode(),
                              invoice.getPurchaserName(), invoice.getVerifyDate(),
                              invoice.getFieldFirst(),invoice.getFieldSecond(),
                              invoice.getFieldThird(),invoice.getVerifystatus(),invoice.getResultId()};
                  openIDS();
                  return executeUpdate(sql, objs);
            } catch (SQLException e) {
                  throw new SQLException("���·�Ʊ������ִ��SQL�����쳣: " + e.getMessage(),
                                                  e);
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
              public Loan isRecord(String appno) throws Exception{
            Loan loan =new Loan();
            DBAccess db = new DBAccess();
            String sql = "select application_no,isused,dealer_channel_flag from loan where application_no=?";
            try {
                  Object[] objs = {appno};
                  openIDS();
                  executeQuery(sql, objs);
                  if(rs.next()) {
                        loan.setApplication_no(rs.getString("application_no"));
                        loan.setIsused(rs.getString("isused"));
                        loan.setDealer_channel_flag(rs.getString("dealer_channel_flag"));
                  }
                  return loan;
            } catch (SQLException e) {
                  throw e;
            } finally {
                  try {
                        closeDB();
                  } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                  }
            }
      }
 
}
