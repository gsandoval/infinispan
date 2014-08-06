package org.infinispan.util.logging.log4j;

import java.util.regex.Pattern;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.core.filter.ThresholdFilter;

/**
 * Log4j {@link Filter} that only allow events from threads matching a regular expression.
 * Events with a level greater than {@code threshold} are always logged.
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Plugin(name = "ThreadNameFilter", category = "Core", elementType = "filter", printObject = false)
public class ThreadNameFilter extends AbstractFilter {
   private Level threshold = Level.DEBUG;
   private Pattern includePattern;

   public Level getThreshold() {
      return threshold;
   }

   public void setThreshold(Level threshold) {
      this.threshold = threshold;
   }

   public String getInclude() {
      return includePattern != null ? includePattern.pattern() : null;
   }

   public void setInclude(String include) {
      this.includePattern = Pattern.compile(include);
   }

   @Override
   public Result filter(LogEvent event) {
      if (event.getLevel().isMoreSpecificThan(threshold)) {
         return Filter.Result.NEUTRAL;
      } else if (includePattern == null || includePattern.matcher(event.getThreadName()).find()) {
         return Filter.NEUTRAL;
      } else {
         return Filter.DENY;
      }
   }

   @PluginFactory
   public static ThreadNameFilter createFilter(@PluginAttribute("threshold") String threshold,
                                               @PluginAttribute("include") String include) {
      Level level = threshold == null ? Level.WARN : Level.toLevel(threshold.toUpperCase());
      Pattern includePattern = Pattern.compile(include);

      return new ThreadNameFilter(level, includePattern);
   }
}
