package uk.ac.gla.terrier.probos.controller;

import java.io.Closeable;
import java.io.File;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.ProcessBuilder.Redirect;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

import uk.ac.gla.terrier.probos.PConfiguration;

public abstract class MailClient implements Closeable {
	
	public static MailClient getMailClient(Configuration pconf)
	{
		String DEFAULT = SendmailClient.class.getSimpleName();
		if (pconf.get(PConfiguration.KEY_MAIL_CLIENT, DEFAULT).equals(DEFAULT))
			return new LoggingMailClient(new SendmailClient(pconf));
		return new LoggingMailClient(new JavaXMailClient(pconf));
	}
	
	public abstract boolean sendEmail(String destination, String subject, String[] content);
	public void close() {}
	
	static class LoggingMailClient extends MailClient
	{
		private static final Log LOG = LogFactory.getLog(MailClient.class);
		MailClient parent;
		LoggingMailClient(MailClient _parent)
		{
			this.parent = _parent;
		}		
		
		@Override
		public boolean sendEmail(String destination, String subject,
				String[] content) {
			LOG.info("Sending email to '" + destination + "' about '" + subject + "' using " + parent.getClass().getSimpleName());
			boolean rtr = parent.sendEmail(destination, subject, content);
			LOG.info("Sent email success? " + rtr);
			return rtr;
		}		
	}
	
	static class JavaXMailClient extends MailClient
	{
		Properties mProps = new Properties();
		String from;
		String login;
		String password;
		
		public JavaXMailClient(Configuration pconf)
		{
			from = pconf.get(PConfiguration.KEY_MAIL_FROM);
			mProps.put("mail.debug", "true");
			mProps.put("mail.smtp.host", pconf.get(PConfiguration.KEY_MAIL_HOST));
			mProps.put("mail.smtp.port", pconf.get(PConfiguration.KEY_MAIL_HOST_PORT, "25"));
			
			login = pconf.get(PConfiguration.KEY_MAIL_HOST_LOGIN);
			password = pconf.get(PConfiguration.KEY_MAIL_HOST_PASSWORD);
			if (login != null)
				mProps.put("mail.smtp.auth", "true");
			mProps.put("mail.smtp.starttls.enable", pconf.get(PConfiguration.KEY_MAIL_HOST_TLS, "false"));
			
		}
		
		@Override
		public boolean sendEmail(String destination, String subject, String[] content)
		{
			// Get session
			Session session = login != null
					? Session.getInstance(mProps,
							  new javax.mail.Authenticator() {
						protected PasswordAuthentication getPasswordAuthentication() {
							return new PasswordAuthentication(login, password);
						}
					  })
					: Session.getInstance(mProps);
			try {
			    // Instantiate a message
			    Message msg = new MimeMessage(session);
			 
			    // Set the FROM message
			    msg.setFrom(new InternetAddress(from));
			    
			    // The recipients can be more than one so we use an array but you can
			    // use 'new InternetAddress(to)' for only one address.
			    InternetAddress[] address = {new InternetAddress(destination)};
			    msg.setRecipients(Message.RecipientType.TO, address);
			 
			    // Set the message subject and date we sent it.
			    msg.setSubject(subject);
			    msg.setSentDate(new Date());
			 
			    // Set message content
			    msg.setText(StringUtils.join("\n", content));
			 
			    // Send the message
			    Transport.send(msg);
			    return true;
			}
			catch (MessagingException mex) {
			    mex.printStackTrace();
			    return false;
			}
			
		}
		
	}
	
	static String whichExists(String... paths)
	{
		for(String s : paths)
		{
			if (new File(s).exists())
				return s;
		}
		return null;
	}
	
	static class SendmailClient extends MailClient
	{
		final String sendmail;
		static final long TIMEOUT_MS = 10000l;
		ExecutorService service = Executors.newSingleThreadExecutor();
		
		public SendmailClient(Configuration conf)
		{
			sendmail = conf.get(PConfiguration.KEY_MAIL_SENDMAIL, 
				whichExists("/usr/lib/sendmail", "/usr/sbin/sendmail"));
		}
		
		@Override public void close() {
			service.shutdownNow();
		}
		
		@Override
		public boolean sendEmail(final String destination, final String subject,
				final String[] content) {
			
			if (sendmail == null)
				return true;
			
			Future<Boolean> success = service.submit(new Callable<Boolean>() {
				@Override
				public Boolean call() throws Exception {
					try{
						ProcessBuilder pb = new ProcessBuilder();
						pb.command(sendmail);
						pb.redirectInput(Redirect.PIPE);
						Process p = pb.start();
						PrintWriter os = new PrintWriter(new OutputStreamWriter(p.getOutputStream()));
						os.println();
						os.println("Subject: " + subject);
						os.println();
						for(String c : content)
							os.println(c);
						os.close();
						p.waitFor();				
						return p.exitValue() == 0;
					
					} catch (Exception e) {
						e.printStackTrace();
						return false;
					}
				}
				
			});
			boolean rtr;
			try {
				rtr = success.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
			} catch (Exception e) {
				e.printStackTrace();
				rtr = false;
			}
			return rtr;			
		}
		
	}
	
}
