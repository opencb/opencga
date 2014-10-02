package org.opencb.opencga.lib.common;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Date;
import java.util.Properties;

public class MailUtils {

    public static void sendResetPasswordMail(String to, String newPassword, final String mailUser, final String mailPassword, String mailHost, String mailPort) {


        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", mailHost);
        props.put("mail.smtp.port", mailPort);

        Session session = Session.getInstance(props,
                new javax.mail.Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(mailUser, mailPassword);
                    }
                });

        try {

            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress(mailUser));
            message.setRecipients(Message.RecipientType.TO,
                    InternetAddress.parse(to));

            message.setSubject("Your password has been reset");
            message.setText("Hello, \n" +
                    "You can now login using this new password:" +
                    "\n\n" +
                    newPassword +
                    "\n\n\n" +
                    "Please change it when you first login" +
                    "\n\n" +
                    "Best regards,\n\n" +
                    "Systems Genomics Laboratory" +
                    "\n");

            Transport.send(message);

            System.out.println("Password reset: " + to);

        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }
    }

    public static void sendMail(String smtpServer, String to, String from, String subject, String body) {
        try {
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
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
