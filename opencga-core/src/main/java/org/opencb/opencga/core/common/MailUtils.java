/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.core.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.Message;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Date;
import java.util.Properties;

public class MailUtils {

    private static final Logger logger = LoggerFactory.getLogger(MailUtils.class);

    public static void sendResetPasswordMail(String to, String newPassword, final String mailUser, final String mailPassword,
                                             String mailHost, String mailPort, String userId) throws Exception {

        Properties props = new Properties();
        props.put("mail.smtp.host", mailHost);
        props.put("mail.smtp.port", mailPort);
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.ssl.enable", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.starttls.required", "true");
        props.put("mail.smtp.ssl.protocols", "TLSv1.2");
        props.put("mail.smtp.ssl.checkserveridentity", "true");
        props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");

        Session session = Session.getInstance(props,
                new javax.mail.Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(mailUser, mailPassword);
                    }
                });
        logger.info("Sending reset password from" + mailUser + " to " + to + " using " + mailHost + ":" + mailPort);
        Message message = new MimeMessage(session);
        message.setFrom(new InternetAddress(mailUser));
        message.setRecipients(Message.RecipientType.TO,
                InternetAddress.parse(to));

        message.setSubject("XetaBase: Password Reset");
        message.setText(getEmailContent(userId,newPassword));
        Transport.send(message);
    }

    public static String getEmailContent(String userId, String temporaryPassword) {
        StringBuilder sb = new StringBuilder();

        sb.append("Hi ").append(userId).append(",\n\n");
        sb.append("We confirm that your password has been successfully reset.\n\n");
        sb.append("Please find your new login credentials below:\n\n");
        sb.append("User ID: ").append(userId).append("\n");
        sb.append("Temporary Password: ").append(temporaryPassword).append("\n\n");
        sb.append("For your security, we strongly recommend that you log in using the temporary password provided ");
        sb.append("and promptly create a new password that is unique and known only to you. ");
        sb.append("You can change your password by accessing \"Your Profile > Change Password\" in your User Profile.\n\n");
        sb.append("If you did not request a password reset, please contact our support team immediately at support@zettagenomics.com.\n\n");
        sb.append("Best regards,\n\n");
        sb.append("ZettaGenomics Support Team \n\n");



        return sb.toString();
    }

    public static void sendMail(String smtpServer, String to, String from, String subject, String body) throws Exception {

        Properties props = System.getProperties();
        // -- Attaching to default Session, or we could start a new one --
        props.put("mail.smtp.host", smtpServer);
        Session session = Session.getDefaultInstance(props, null);
        // -- Create a new message --
        // Message msg = new javax.mail.Message(session);
        Message msg = new MimeMessage(session);
        // -- Set the FROM and TO fields --
        msg.setFrom(new InternetAddress(from));
        msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to, false));
        // -- We could include CC recipients too --
        // if (cc != null)
        // msg.setRecipients(Message.RecipientType.CC
        // ,InternetAddress.parse(cc, false));
        // -- Set the subject and body text --
        msg.setSubject(subject);
        msg.setText(body);
        // -- Set some other header information --
        msg.setHeader("X-Mailer", "LOTONtechEmail");
        msg.setSentDate(new Date());
        // -- Send the message --
        Transport.send(msg);
        System.out.println("Message sent OK.");

    }
}
