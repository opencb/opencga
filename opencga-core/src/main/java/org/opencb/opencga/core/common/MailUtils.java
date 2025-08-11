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

import org.opencb.opencga.core.config.Email;
import org.opencb.opencga.core.exceptions.MailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.Message;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

public class MailUtils {

    private static final Logger logger = LoggerFactory.getLogger(MailUtils.class);

    private final Email email;

    private MailUtils(Email emailConfig) {
        this.email = emailConfig;
    }

    public static MailUtils configure(Email emailConfig) {
        return new MailUtils(emailConfig);
    }

    public void sendMail(String targetMail, String subject, String content) throws MailException {
        Properties props = new Properties();
        props.put("mail.smtp.host", email.getHost());
        props.put("mail.smtp.port", email.getPort());
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
                        return new PasswordAuthentication(email.getUser(), email.getPassword());
                    }
                });
        logger.info("Sending email from '{}' to '{}' using '{}:{}'", email.getUser(), targetMail, email.getHost(), email.getPort());

        try {
            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress(email.getUser()));
            message.setRecipients(Message.RecipientType.TO,
                    InternetAddress.parse(targetMail));

            message.setSubject(subject);
            message.setText(content);
            Transport.send(message);
        } catch (Exception e) {
            throw new MailException("Could not send email.", e);
        }
    }

    public static String getResetMailContent(String userId, String temporaryPassword) {
        return new StringBuilder()
                .append("Hi ").append(userId).append(",\n\n")
                .append("We confirm that your password has been successfully reset.\n\n")
                .append("Please find your new login credentials below:\n\n")
                .append("User ID: ").append(userId).append("\n")
                .append("Temporary Password: ").append(temporaryPassword).append("\n\n")
                .append("For your security, we strongly recommend that you log in using the temporary password provided ")
                .append("and promptly create a new password that is unique and known only to you. ")
                .append("You can change your password by accessing \"Your Profile > Change Password\" in your User Profile.\n\n")
                .append("If you did not request a password reset, please contact our support team immediately at support@zettagenomics.com.\n\n")
                .append("Best regards,\n\n")
                .append("ZettaGenomics Support Team \n\n")
                .toString();
    }

}
