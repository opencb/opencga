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

package org.opencb.opencga.catalog.managers;

import org.opencb.opencga.core.config.Email;

import javax.mail.Message;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

public class MailManager {

    private final Email emailConfig;
    private String mailUser;
    private String mailPassword;
    private String mailHost;
    private String mailPort;


    public MailManager(Email emailConfig) {
        this.emailConfig = emailConfig;
        this.mailUser = emailConfig.getUser();
        this.mailPassword = emailConfig.getPassword();
        this.mailHost = emailConfig.getHost();
        this.mailPort = emailConfig.getPort();
    }

    public void sendResetPasswordMail(String to, String newPassword) throws Exception {
        sendResetPasswordMail(to, newPassword, mailUser, mailPassword,
                mailHost, mailPort, "true");
    }

    public void sendResetPasswordMail(String to, String newPassword, final String mailUser, final String mailPassword,
                                      String mailHost, String mailPort, String ssl) throws Exception {

        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", ssl);
        props.put("mail.smtp.host", mailHost);
        props.put("mail.smtp.port", mailPort);

        Session session = Session.getInstance(props,
                new javax.mail.Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(mailUser, mailPassword);
                    }
                });
        //    logger.info("Sending reset password from" + mailUser + " to " + to + " using " + mailHost + ":" + mailPort);
        Message message = new MimeMessage(session);
        message.setFrom(new InternetAddress(mailUser));
        message.setRecipients(Message.RecipientType.TO,
                InternetAddress.parse(to));

        message.setSubject("Your password has been reset");
        message.setText("Hello, \n"
                + "You can now login using this new password:"
                + "\n\n"
                + newPassword
                + "\n\n\n"
                + "Please change it when you first login"
                + "\n\n"
                + "Best regards,\n\n"
                + "Systems Genomics Laboratory"
                + "\n");

        Transport.send(message);

        System.out.println("Password reset: " + to);
    }

    public String getMailUser() {
        return mailUser;
    }

    public void setMailUser(String mailUser) {
        this.mailUser = mailUser;
    }

    public String getMailPassword() {
        return mailPassword;
    }

    public void setMailPassword(String mailPassword) {
        this.mailPassword = mailPassword;
    }

    public String getMailHost() {
        return mailHost;
    }

    public void setMailHost(String mailHost) {
        this.mailHost = mailHost;
    }

    public String getMailPort() {
        return mailPort;
    }

    public void setMailPort(String mailPort) {
        this.mailPort = mailPort;
    }
}
