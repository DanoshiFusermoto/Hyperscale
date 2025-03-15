package org.radix.hyperscale.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;

public class TimeSeriesStatistics {
  private final class Record implements Comparable<Record> {
    private final long timestamp;
    private final Map<String, MutableDouble> fields;

    Record(final long timestamp) {
      this.timestamp = timestamp;
      this.fields = Maps.mutable.empty();
    }

    long get(final String field, final long _default) {
      final MutableDouble entry = this.fields.get(field);
      if (entry == null) return _default;

      return entry.longValue();
    }

    double get(final String field, final double _default) {
      final MutableDouble entry = (MutableDouble) this.fields.get(field);
      if (entry == null) return _default;

      return entry.doubleValue();
    }

    double increment(final String field, final long value) {
      final MutableDouble entry = this.fields.computeIfAbsent(field, s -> new MutableDouble(0));
      return entry.addAndGet(value);
    }

    double increment(final String field, final double value) {
      final MutableDouble entry = this.fields.computeIfAbsent(field, s -> new MutableDouble(0));
      return entry.addAndGet(value);
    }

    void put(final String field, final long value) {
      final MutableDouble entry = this.fields.computeIfAbsent(field, s -> new MutableDouble(0));
      entry.setValue(value);
    }

    void put(final String field, final double value) {
      final MutableDouble entry = this.fields.computeIfAbsent(field, s -> new MutableDouble(0));
      entry.setValue(value);
    }

    @Override
    public int compareTo(final Record other) {
      if (this.timestamp < other.timestamp) return -1;
      if (this.timestamp > other.timestamp) return 1;

      return 0;
    }
  }

  private final long maxAge;
  private final Set<String> fields;
  private final MutableLongObjectMap<Record> records;

  private transient volatile long lastPurge = 0;
  private final transient Semaphore purging = new Semaphore(1);

  public TimeSeriesStatistics(final long duration, final TimeUnit unit) {
    // Convert max time to seconds
    this.maxAge = unit.toSeconds(duration);

    this.fields = Sets.mutable.empty();
    this.records = LongObjectMaps.mutable.empty();
  }

  public TimeSeriesStatistics() {
    this(15, TimeUnit.MINUTES);
  }

  public List<String> getFields() {
    synchronized (this.records) {
      return Collections.unmodifiableList(new ArrayList<String>(this.fields));
    }
  }

  private void purgeAged() {
    final long now = System.currentTimeMillis();
    if (now - this.lastPurge > TimeUnit.SECONDS.toMillis(1)) {
      if (this.purging.tryAcquire() == false) return;

      try {
        final long nowSeconds = TimeUnit.MILLISECONDS.toSeconds(now);
        synchronized (this.records) {
          final Iterator<Record> recordsIterator = this.records.values().iterator();
          while (recordsIterator.hasNext()) {
            final Record record = recordsIterator.next();
            if (nowSeconds - record.timestamp > this.maxAge) {
              //							this.range.remove(record);
              recordsIterator.remove();
            }
          }
        }
      } finally {
        this.lastPurge = now;
        this.purging.release();
      }
    }
  }

  public void put(final String field, final long value, final long timestamp, final TimeUnit unit) {
    final long timestampSeconds = unit.toSeconds(timestamp);
    synchronized (this.records) {
      Record record = this.records.get(timestampSeconds);
      if (record == null) {
        record = new Record(timestampSeconds);
        this.records.put(timestampSeconds, record);
        //				this.range.add(record);
      }

      record.put(field, value);
      this.fields.add(field);

      purgeAged();
    }
  }

  public void put(
      final String field, final double value, final long timestamp, final TimeUnit unit) {
    final long timestampSeconds = unit.toSeconds(timestamp);
    synchronized (this.records) {
      Record record = this.records.get(timestampSeconds);
      if (record == null) {
        record = new Record(timestampSeconds);
        this.records.put(timestampSeconds, record);
        //				this.range.add(record);
      }

      record.put(field, value);
      this.fields.add(field);

      purgeAged();
    }
  }

  public long get(
      final String field, final long _default, final long timestamp, final TimeUnit unit) {
    final long timestampSeconds = unit.toSeconds(timestamp);
    synchronized (this.records) {
      final Record record = this.records.get(timestampSeconds);
      if (record == null) return _default;

      return record.get(field, _default);
    }
  }

  public double get(
      final String field, final double _default, final long timestamp, final TimeUnit unit) {
    final long timestampSeconds = unit.toSeconds(timestamp);
    synchronized (this.records) {
      final Record record = this.records.get(timestampSeconds);
      if (record == null) return _default;

      return record.get(field, _default);
    }
  }

  public long increment(
      final String field, final long value, final long timestamp, final TimeUnit unit) {
    final long timestampSeconds = unit.toSeconds(timestamp);
    synchronized (this.records) {
      Record record = this.records.get(timestampSeconds);
      if (record == null) {
        record = new Record(timestampSeconds);
        this.records.put(timestampSeconds, record);
        //				this.range.add(record);
      }

      final double incremented = record.increment(field, value);
      this.fields.add(field);
      purgeAged();

      return (long) incremented;
    }
  }

  public double increment(
      final String field, final double value, final long timestamp, final TimeUnit unit) {
    final long timestampSeconds = unit.toSeconds(timestamp);
    synchronized (this.records) {
      Record record = this.records.get(timestampSeconds);
      if (record == null) {
        record = new Record(timestampSeconds);
        this.records.put(timestampSeconds, record);
      }

      final double incremented = record.increment(field, value);
      this.fields.add(field);
      purgeAged();

      return incremented;
    }
  }

  public double average(final String field, final long start, final TimeUnit unit) {
    return average(
        field, start, unit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS), unit);
  }

  public double average(final String field, final long start, final long end, final TimeUnit unit) {
    double total = 0;
    final long startSeconds = unit.toSeconds(start);
    final long endSeconds = unit.toSeconds(end);
    final long slices = endSeconds - startSeconds;
    synchronized (this.records) {
      for (long t = startSeconds; t < endSeconds; t++) {
        final Record record = this.records.get(t);
        if (record == null) continue;

        final double value = record.get(field, 0);
        total += value;
      }
    }

    return total / slices;
  }

  public double sum(final String field, final long start, final TimeUnit unit) {
    return sum(field, start, unit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS), unit);
  }

  public double sum(final String field, final long start, final long end, final TimeUnit unit) {
    double total = 0;
    final long startSeconds = unit.toSeconds(start);
    final long endSeconds = unit.toSeconds(end);
    synchronized (this.records) {
      for (long t = startSeconds; t < endSeconds; t++) {
        final Record record = this.records.get(t);
        if (record == null) continue;

        final double value = record.get(field, 0);
        total += value;
      }
    }

    return total;
  }

  public double mean(
      final String field, final int maxSamples, final long timestamp, final TimeUnit unit) {
    double total = 0;
    long samples = 0;
    final long endSeconds = unit.toSeconds(timestamp);
    synchronized (this.records) {
      for (long t = endSeconds; t > endSeconds - maxAge; t--) {
        if (samples >= maxSamples) break;

        final Record record = this.records.get(t);
        if (record == null) continue;

        final double value = record.get(field, 0);
        if (value == 0) continue;

        total += value;
        samples++;
      }
    }

    if (samples == 0) return 0;

    return total / samples;
  }
}
