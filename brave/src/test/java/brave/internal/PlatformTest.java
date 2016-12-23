package brave.internal;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
// Added to declutter console: tells power mock not to mess with implicit classes we aren't testing
@PowerMockIgnore({"org.apache.logging.*", "javax.script.*"})
@PrepareForTest(Platform.class)
public class PlatformTest {

  @Test public void relativeTimestamp_incrementsAccordingToNanoTick() {
    mockStatic(System.class);
    when(System.currentTimeMillis()).thenReturn(0L);
    when(System.nanoTime()).thenReturn(0L);

    Platform platform = new Platform() {
      @Override public long randomLong() {
        return 1L;
      }
    };

    when(System.nanoTime()).thenReturn(1000L); // 1 microsecond

    assertThat(platform.epochMicros()).isEqualTo(1);
  }
}
