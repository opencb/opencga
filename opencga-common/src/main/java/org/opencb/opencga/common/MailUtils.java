package org.opencb.opencga.common;

public class MailUtils {
    public static void sendResetPasswordMail(String to, String message) {
        sendMail("correo.cipf.es", to, "babelomics@cipf.es", "Genomic cloud storage analysis password reset",
                message.toString());
    }

    public static void sendMail(String smtpServer, String to, String from, String subject, String body) {
        try {
//			Properties props = System.getProperties();
//			// -- Attaching to default Session, or we could start a new one --
//			props.put("mail.smtp.host", smtpServer);
//			javax.mail.Session session = javax.mail.Session.getDefaultInstance(props, null);
//			// -- Create a new message --
//			// Message msg = new javax.mail.Message(session);
//			Message msg = new MimeMessage(session);
//			// -- Set the FROM and TO fields --
//			msg.setFrom(new InternetAddress(from));
//			msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to, false));
//			// -- We could include CC recipients too --
//			// if (cc != null)
//			// msg.setRecipients(Message.RecipientType.CC
//			// ,InternetAddress.parse(cc, false));
//			// -- Set the subject and body text --
//			msg.setSubject(subject);
//			msg.setText(body);
//			// -- Set some other header information --
//			msg.setHeader("X-Mailer", "LOTONtechEmail");
//			msg.setSentDate(new Date());
//			// -- Send the message --
//			Transport.send(msg);
            System.out.println("Message sent OK.");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
