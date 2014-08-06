package org.infinispan.util.logging.log4j;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.appender.AbstractOutputStreamAppender;
import org.apache.logging.log4j.core.appender.FileManager;
import org.apache.logging.log4j.core.appender.ManagerFactory;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginConfiguration;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.core.net.Advertiser;
import org.apache.logging.log4j.core.util.Booleans;
import org.apache.logging.log4j.core.util.Integers;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Appender that writes to a file and compresses the output using gzip.
 *
 * Based on <code>org.apache.log4j.FileAppender</code>
 */
@Plugin(name = "CompressedFile", category = "Core", elementType = "appender", printObject = false)
public final class CompressedFileAppender extends AbstractOutputStreamAppender<FileManager> {

   private static final int DEFAULT_BUFFER_SIZE = 8192;
   private final String fileName;
   private final Advertiser advertiser;
   private Object advertisement;

   private CompressedFileAppender(final String name, final Layout<? extends Serializable> layout, final Filter filter, final FileManager manager,
         final String filename, final boolean ignoreExceptions, final boolean immediateFlush,
         final Advertiser advertiser) {
      super(name, layout, filter, ignoreExceptions, immediateFlush, manager);
      if (advertiser != null) {
         final Map<String, String> configuration = new HashMap<String, String>(layout.getContentFormat());
         configuration.putAll(manager.getContentFormat());
         configuration.put("contentType", layout.getContentType());
         configuration.put("name", name);
         advertisement = advertiser.advertise(configuration);
      }
      this.fileName = filename;
      this.advertiser = advertiser;
   }

   @Override
   public void stop() {
      super.stop();
      if (advertiser != null) {
         advertiser.unadvertise(advertisement);
      }
   }

   /**
    * Returns the file name this appender is associated with.
    * @return The File name.
    */
   public String getFileName() {
      return this.fileName;
   }

   /**
    * Create a File Appender.
    * @param fileName The name and path of the file.
    * @param name The name of the Appender.
    * @param immediateFlush "true" if the contents should be flushed on every write, "false" otherwise. The default
    * is "true".
    * @param ignore If {@code "true"} (default) exceptions encountered when appending events are logged; otherwise
    *               they are propagated to the caller.
    * @param bufferedIo "true" if I/O should be buffered, "false" otherwise. The default is "true".
    * @param bufferSizeStr buffer size for buffered IO (default is 8192).
    * @param layout The layout to use to format the event. If no layout is provided the default PatternLayout
    * will be used.
    * @param filter The filter, if any, to use.
    * @param advertise "true" if the appender configuration should be advertised, "false" otherwise.
    * @param advertiseUri The advertised URI which can be used to retrieve the file contents.
    * @param config The Configuration
    * @return The FileAppender.
    */
   @PluginFactory
   public static CompressedFileAppender createAppender(
         // @formatter:off
         @PluginAttribute("fileName") final String fileName,
         @PluginAttribute("name") final String name,
         @PluginAttribute("immediateFlush") final String immediateFlush,
         @PluginAttribute("ignoreExceptions") final String ignore,
         @PluginAttribute("bufferedIo") final String bufferedIo,
         @PluginAttribute("bufferSize") final String bufferSizeStr,
         @PluginElement("Layout") Layout<? extends Serializable> layout,
         @PluginElement("Filter") final Filter filter,
         @PluginAttribute("advertise") final String advertise,
         @PluginAttribute("advertiseUri") final String advertiseUri,
         @PluginConfiguration final Configuration config) {
      // @formatter:on
      boolean isBuffered = Booleans.parseBoolean(bufferedIo, true);
      final boolean isAdvertise = Boolean.parseBoolean(advertise);
      final int bufferSize = Integers.parseInt(bufferSizeStr, DEFAULT_BUFFER_SIZE);
      if (!isBuffered && bufferSize > 0) {
         LOGGER.warn("The bufferSize is set to {} but bufferedIO is not true: {}", bufferSize, bufferedIo);
      }
      final boolean isFlush = Booleans.parseBoolean(immediateFlush, true);
      final boolean ignoreExceptions = Booleans.parseBoolean(ignore, true);

      if (name == null) {
         LOGGER.error("No name provided for FileAppender");
         return null;
      }

      if (fileName == null) {
         LOGGER.error("No filename provided for FileAppender with name "  + name);
         return null;
      }
      if (layout == null) {
         layout = PatternLayout.createDefaultLayout();
      }

      final FileManager manager = CompressedFileManager.getCompressedFileManager(fileName, isBuffered, advertiseUri,
            layout, bufferSize);
      if (manager == null) {
         return null;
      }

      return new CompressedFileAppender(name, layout, filter, manager, fileName, ignoreExceptions, isFlush,
            isAdvertise ? config.getAdvertiser() : null);
   }

   private static class CompressedFileManager extends FileManager {
      private static final CompressedFileManagerFactory FACTORY = new CompressedFileManagerFactory();
      protected CompressedFileManager(String fileName, OutputStream os, String advertiseURI, Layout layout, int bufferSize) {
         super(fileName, os, false, false, advertiseURI, layout, bufferSize);
      }

      public static CompressedFileManager getCompressedFileManager(final String fileName, final boolean bufferedIO,
            String advertiseURI, Layout layout, int bufferSize) {
         FactoryData factoryData = new FactoryData(bufferedIO, advertiseURI, layout, bufferSize);
         return (CompressedFileManager) getManager(fileName, factoryData, FACTORY);
      }
      super.reset();
   }

   /**
    * Factory Data.
    */
   private static class FactoryData {
      private final boolean bufferedIO;
      private final String advertiseURI;
      private final Layout layout;
      private final int bufferSize;

      public FactoryData(final boolean bufferedIO, String advertiseURI, Layout layout, int bufferSize) {
         this.bufferedIO = bufferedIO;
         this.advertiseURI = advertiseURI;
         this.layout = layout;
         this.bufferSize = bufferSize;
      }
   }

   /**
    * Factory to create a FileManager.
    */
   private static class CompressedFileManagerFactory implements ManagerFactory<FileManager, FactoryData> {

      /**
       * Create a FileManager.
       * @param name The name of the File.
       * @param data The FactoryData
       * @return The FileManager for the File.
       */
      public FileManager createManager(final String name, final FactoryData data) {
         final File file = new File(name);
         final File parent = file.getParentFile();
         if (null != parent && !parent.exists()) {
            parent.mkdirs();
         }

         OutputStream os;
         try {
            os = new FileOutputStream(name, false);
            if (data.bufferedIO) {
               os = new BufferedOutputStream(os);
            }
            return new CompressedFileManager(name, os, data.advertiseURI, data.layout, data.bufferSize);
         } catch (final FileNotFoundException ex) {
            LOGGER.error("FileManager (" + name + ") " + ex);
         }
         return null;
      }
   }
}
