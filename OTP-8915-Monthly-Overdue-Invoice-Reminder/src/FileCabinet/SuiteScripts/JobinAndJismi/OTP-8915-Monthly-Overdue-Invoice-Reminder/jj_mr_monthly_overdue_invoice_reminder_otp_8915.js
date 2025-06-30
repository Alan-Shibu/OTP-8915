/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 * @NModuleScope SameAccount
 */
/*************************************************************************************
 *
 *
 * ${OTP-8915} : ${Monthly Over Due Reminder for Customer}
 *
 *
 **************************************************************************************
 *
 * Author: Jobin and Jismi IT Services
 *
 * Date Created : 05-June-2025
 *
 * Description : This script is for sending monthly reminder emails to the customers
 * regarding their overdue invoices. A CSV file containing the customer name , email
 * address , invoice document number, invoice amount and the number of days overdue
 * will be attached with mail.The mail will be sent by the sales reps to their
 * respective customer.In case any customer does not have a sales rep assigned, the
 * mail will be sent by a static NetSuite admin.
 *
 * REVISION HISTORY
 *
 * @version 1.0  05-June-2025 : The initial build was created by JJ0401
 *
 *
 *************************************************************************************/
define(["N/email", "N/file", "N/log", "N/search"], /**
 * @param{email} email
 * @param{file} file
 * @param{log} log
 * @param{search} search
 */ (email, file, log, search) => {
  /**
   * Defines the function that is executed at the beginning of the map/reduce process and
   * generates the input data.
   * @param {Object} inputContext
   * @param {boolean} inputContext.isRestarted - Indicates whether the current invocation
   *                                             of this function is the first invocation
   *                                             (if true, the current invocation is not the
   *                                             first invocation and this function has been
   *                                             restarted)
   * @param {Object} inputContext.ObjectRef -    Object that references the input data
   * @typedef {Object} ObjectRef
   * @property {string|number} ObjectRef.id -   Internal ID of the record instance that
   *                                            contains the input data
   * @property {string} ObjectRef.type -        Type of the record instance that contains
   *                                            the input data
   * @returns {Array|Object|Search|ObjectRef|File|Query} The input data to use in the map/
   *                                            reduce
   *                                            process
   * @since 2015.2
   */

  const getInputData = (inputContext) => {
    try {
      let invoiceData = fetchData();
      return invoiceData;
    } catch (e) {
      log.debug("Error Caught", e.message);
    }
  };

  /**
   * Defines the function that is executed when the map entry point is triggered. This entry
   * point is triggered automatically when the associated getInputData stage is complete.
   * This function is applied to each key-value pair in the provided context.
   * @param {Object} mapContext -             Data collection containing the key-value pairs to
   *                                          process  in the map stage. This parameter is
   *                                          provided automatically based on the results of the
   *                                          getInputData stage.
   * @param {Iterator} mapContext.errors -    Serialized errors that were thrown during previous
   *                                          attempts to execute the map function on the current
   *                                          key-value pair
   * @param {number} mapContext.executionNo - Number of times the map function has been executed
   *                                          on the current key-value pair
   * @param {boolean} mapContext.isRestarted -Indicates whether the current invocation of this
   *                                          function is the first invocation (if true, the
   *                                          current invocation is not the first invocation and
   *                                          this function has been restarted)
   * @param {string} mapContext.key -         Key to be processed during the map stage
   * @param {string} mapContext.value -       Value to be processed during the map stage
   * @since 2015.2
   */

  const map = (mapContext) => {
    try {
      let invoiceData = JSON.parse(mapContext.value);
      let customerId = invoiceData.values.entity[0].value;

      mapContext.write({
        key: customerId,
        value: invoiceData,
      });
    } catch (e) {
      log.error("Error caught", e.message);
    }
  };

  /**
   * Defines the function that is executed when the reduce entry point is triggered. This entry point
   * is triggered automatically when the associated map stage is complete. This function is applied to
   * each group in the provided context.
   * @param {Object} reduceContext -            Data collection containing the groups to process in the
   *                                            reduce stage. This parameter is provided automatically
   *                                            based on the results of the map stage.
   * @param {Iterator} reduceContext.errors -   Serialized errors that were thrown during previous
   *                                            attempts to execute the reduce function on the current
   *                                            group
   * @param {number} reduceContext.executionNo -Number of times the reduce function has been executed
   *                                            on the current group
   * @param {boolean} reduceContext.isRestarted-Indicates whether the current invocation of thisfunction
   *                                            is the first invocation (if true, the current invocation
   *                                            is not the first invocation and this function has been
   *                                            restarted)
   * @param {string} reduceContext.key -        Key to be processed during the reduce stage
   * @param {List<String>} reduceContext.values -All values associated with a unique key that was passed
   *                                             to the reduce stage for processing
   * @since 2015.2
   */
  const reduce = (reduceContext) => {
    try {
      let csvName = ``;
      let csvContent = `Customer Name,Customer Email,Invoice Number,Invoice Amount,Number of days overdue \n`;
      let salesRep = "";
      let customerName = "";
      let fileAttachment;
      reduceContext.values.forEach((value) => {
        let parsedData = JSON.parse(value);
        csvContent += ` ${parsedData.values.entity[0].text}, ${parsedData.values.email}, ${parsedData.values.tranid}, ${parsedData.values.amount}, ${parsedData.values.daysoverdue} \n`;
        csvName = `Overdue invoice details of ${parsedData.values.entity[0].text}`;

        let repList = parsedData.values["customer.salesrep"];
        salesRep =
          repList && repList.length > 0 ? repList[0].value : -1;
        customerName = parsedData.values.entity[0].value;

        fileAttachment = fileCreation(
          csvName,
          csvContent,
          salesRep,
          customerName
        );
        return true;
      });

      emailSend(fileAttachment, salesRep, customerName);
    } catch (e) {
      log.error("Error caught", e.message);
    }
  };

  /**
   * Defines the function that is executed when the summarize entry point is triggered. This entry point is
   * triggered automatically when the associated reduce stage is complete. This function is applied to the
   * entire result set.
   * @param {Object} summaryContext             -   Statistics about the execution of a map/reduce script
   * @param {number} summaryContext.concurrency -   Maximum concurrency number when executing parallel tasks
   *                                                for the map/reduce script
   * @param {Date} summaryContext.dateCreated   -   The date and time when the map/reduce script began running
   * @param {boolean} summaryContext.isRestarted -  Indicates whether the current invocation of this function is
   *                                                the first invocation (if true, the current invocation is not
   *                                                the first invocation and this function has been restarted)
   * @param {Iterator} summaryContext.output -      Serialized keys and values that were saved as output during
   *                                                the reduce stage
   * @param {number} summaryContext.seconds -       Total seconds elapsed when running the map/reduce script
   * @param {number} summaryContext.usage         - Total number of governance usage units consumed when
   *                                                running the map/reduce script
   * @param {number} summaryContext.yields        - Total number of yields when running the map/reduce script
   * @param {Object} summaryContext.inputSummary  - Statistics about the input stage
   * @param {Object} summaryContext.mapSummary    - Statistics about the map stage
   * @param {Object} summaryContext.reduceSummary - Statistics about the reduce stage
   * @since 2015.2
   */
  const summarize = (summaryContext) => {
    try {
      log.error("Mails were sent successfully to the customers!");
    } catch (e) {
      log.error("Error caught", e.message);
    }
  };

  /**
   * Function to create a search to fetch the details of open invoices
   * @param
   * @returns {search object}
   */
  function fetchData() {
    try {
      let openInvoices = search.create({
        title: "Search for OTP-8915",
        id: "customsearch_jj_open_invoice_details",
        type: search.Type.INVOICE,
        filters: [
          ["type", "anyof", "CustInvc"],
          "AND",
          ["status", "anyof", "CustInvc:A"],
          "AND",
          ["mainline", "is", "T"],
          "AND",
          ["daysoverdue", "greaterthan", "0"],
          "AND",
          ["trandate", "within", "thismonth"],
          "AND",
          ["customermain.isinactive", "is", "F"],
        ],
        columns: [
          search.createColumn({ name: "entity", label: "Name" }),
          search.createColumn({ name: "email", label: "Email" }),
          search.createColumn({ name: "tranid", label: "Document Number" }),
          search.createColumn({ name: "amount", label: "Amount" }),
          search.createColumn({ name: "daysoverdue", label: "Days Overdue" }),
          search.createColumn({
            name: "salesrep",
            join: "customer",
            label: "Sales Rep",
          }),
        ],
      });

      let data = openInvoices.run().getRange({ start: 0, end: 100 });

      return data;
    } catch (e) {
      log.error("Error caught", e.message);
    }
  }

  /**
   * Function to create a CSV file with the overdue invoice details
   * @param {string} fileName - name of the CSV file
   * @param {string} fileContent - contents of the CSV file
   * @param {int} rep - internal id of the sales rep
   * @param {int} client  - internal id of the customer
   * @returns {void}
   */

  function fileCreation(fileName, fileContent) {
    let fileObj = file.create({
      name: fileName,
      fileType: file.Type.CSV,
      contents: fileContent,
      description:
        "This is a csv file containing the details of overdue invoices.",
      encoding: file.Encoding.UTF8,
      folder: -14,
      isOnline: true,
    });

    fileObj.save();

    return fileObj;
  }

  /**
   * Function to send the overdue invoice reminders to the customers
   * @param {file object} csvFile - the CSV file containing the details of overdue invoices
   * @param {int} saleRep - internal id of the sales rep
   * @param {int} clientName  - internal id of the customer
   * @returns {void}
   */
  function emailSend(csvFile, saleRep, clientId) {
    try {
      let emailAuth;
      let emailRecipient = clientId;
      let emailSubject;
      let emailBody;

     
      
      let repInactive = search.lookupFields({
        type: search.Type.EMPLOYEE,
        id: saleRep,
        columns: ["isinactive"]
      }).isinactive;
      
      let clientName = search.lookupFields({
        type: search.Type.CUSTOMER,
        id: clientId,
        columns: ["entityid"]
      }).entityid;

      if (saleRep === -1)
      {
        emailAuth = -5;
        emailSubject = "Monthly Overdue Invoice Reminder of Customer without assigned/ active sales reps";
        emailBody = `Dear ${clientName},
                     Hope this mail finds you in good health.This is a reminder of your overdue invoices till date.
                     Please go through the CSV file attached along with , which contains the details of the same.
                     Expecting that you will give proper consideration to this mail and take appropriate action soon.
                     Best Regards,
                     Larry`
      }
      else{
        if(repInactive) 
          {
            emailAuth = -5;
            emailSubject = "Monthly Overdue Invoice Reminder of Customer without assigned/ active sales reps";
            emailBody = `Dear ${clientName},
                         Hope this mail finds you in good health.This is a reminder of your overdue invoices till date.
                         Please go through the CSV file attached along with , which contains the details of the same.
                         Expecting that you will give proper consideration to this mail and take appropriate action soon.
                         Best Regards,
                         Larry`
          }
        else{
          emailAuth = saleRep;
          emailSubject = "Monthly Overdue Invoice Reminder";
          emailBody = `Dear ${clientName},
                       Hope this mail finds you in good health.This is a reminder of your overdue invoices till date.
                       Please go through the CSV file attached along with , which contains the details of the same.
                       Expecting that you will give proper consideration to this mail and take appropriate action soon.
                       Best Regards,
                       Larry`
        }
        
      }
       email.send({
          author: emailAuth,
          recipients: [emailRecipient],
          attachments: [csvFile],
          subject: emailSubject,
          body: emailBody,
        });
      } 
      catch (e) {
      log.error("Error caught", e.message);
    }
  }

  return { getInputData, map, reduce, summarize };
});
