package org.hs.rsocketupload;

import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.hs.rsocketupload.domain.Constants;
import org.hs.rsocketupload.domain.Status;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.util.MimeType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import reactor.core.scheduler.Schedulers;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

@Slf4j
@SpringBootTest
class RSocketUploadApplicationTests {

	@Autowired
	private Mono<RSocketRequester> rSocketRequester;

	@Value("classpath:input/todo.pdf")
	private Resource resource;

	static final String FILE_EXT = "epub";
	@Test
	void contextLoads() {
	}

	private Flux<Status> createRequest() {
		return createRequest(null);
	}

	private Flux<Status> createRequest(Integer count) {
		Flux<DataBuffer> readFlux = DataBufferUtils.read(resource, new DefaultDataBufferFactory(), 4096)
				.doOnNext(s -> log.info("Sent"));

		final var cntAsString = Optional.ofNullable(count);

		// rsocket request
		return this.rSocketRequester
				.map(r -> r.route("file.upload")
						.metadata(metadataSpec -> {
							metadataSpec.metadata(FILE_EXT, MimeType.valueOf(Constants.MIME_FILE_EXTENSION));
							metadataSpec.metadata("output" + cntAsString.map(c -> "_" + c).orElse(Strings.EMPTY), MimeType.valueOf(Constants.MIME_FILE_NAME));
						})
						.data(readFlux)
				)
				.flatMapMany(r -> r.retrieveFlux(Status.class))
				.doOnNext(s -> log.info("Upload Status: {}{}", cntAsString.map(c -> c + " ").orElse(Strings.EMPTY), s));
	}

	private static void noop(Status s) {

	}

	@Test
	public void uploadFile() throws InterruptedException {
		final var latch = new CountDownLatch(1);

		// rsocket request
		this.createRequest()
				.subscribe(RSocketUploadApplicationTests::noop, err -> {
					log.error("Subscription failed", err);
					latch.countDown();
				}, latch::countDown);

		latch.await();
	}

	@Test
	public void concurrentUpload() throws InterruptedException {
		final var latch = new CountDownLatch(1);

		Flux.range(1, 10)
				.map(this::createRequest)
//				.publishOn(Schedulers.newParallel("client-pool", 20))
				.flatMap(Function.identity())
				.subscribe(RSocketUploadApplicationTests::noop, err -> {
					log.error("Subscription failed", err);
					latch.countDown();
				}, latch::countDown);

		latch.await();
	}

}
