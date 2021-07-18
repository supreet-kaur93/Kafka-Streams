package services;

import dtos.DoctorSearchedCountDto;
import dtos.HostStoreDto;
import handler.ConstantStrings;
import handler.JsonSerializer;
import javafx.util.Builder;
import javassist.NotFoundException;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import proxy.BaseRestApiManager;

import java.util.*;

@RestController
@RequestMapping(value = "/doctorKafka", produces = MediaType.APPLICATION_JSON_VALUE)
public class DoctorKafkaService {

    private final KafkaStreams streams;
    private final HostInfo hostInfo;

    @Autowired
    BaseRestApiManager baseRestApiManager;

    DoctorKafkaService(final KafkaStreams streams, final HostInfo hostInfo) {
        this.streams = streams;
        this.hostInfo = hostInfo;
    }

    @RequestMapping(value = "/getDashboardData", method = RequestMethod.GET)
    public List<DoctorSearchedCountDto> topFive() throws NotFoundException {
        // The top-five might be hosted elsewhere. There is only one 1 partition with data
        // so we need to first find where it is and then we can do a local or remote lookup.


        final StreamsMetadata metadata =  streams.metadataForKey(ConstantStrings.TOP_FIVE_DOCTORS_IN_AREA_STORE, ConstantStrings
                .TOP_FIVE_KEY, new JsonSerializer<>());

        if (Objects.isNull(metadata)) {
            throw new NotFoundException("NOT FOUND");
        }

        HostStoreDto hostStoreDto = new HostStoreDto(metadata.host(),
                metadata.port(), Collections.singleton(ConstantStrings.TOP_FIVE_DOCTORS_IN_AREA_STORE));

        // top-five is hosted on another instance
        if (!thisHost(hostStoreDto)) {
            baseRestApiManager.get(hostStoreDto.getHost() + ":" + hostStoreDto.getPort(), "kafka-music/charts/top-five/", null, getHttpHeaders(), ArrayList.class);
        }

        // top-five is hosted locally. so lookup in local store
        return topFiveDoctors(ConstantStrings.TOP_FIVE_KEY, ConstantStrings.TOP_FIVE_DOCTORS_STORE);
    }

    private List<DoctorSearchedCountDto> topFiveDoctors(String key, String store) throws NotFoundException {
        ReadOnlyKeyValueStore<String, DoctorKafkaService.TopFiveDoctors> topFiveDoctorsMap = streams.store(StoreQueryParameters.fromNameAndType(store, QueryableStoreTypes.keyValueStore()));
        final DoctorKafkaService.TopFiveDoctors topFiveDoctors = topFiveDoctorsMap.get(key);

        if (topFiveDoctors == null) {
            throw new NotFoundException(String.format("Unable to find count in %s for key %s", store, key));
        }
        final List<DoctorSearchedCountDto> results = new ArrayList<>();
        for (final DoctorDetails doctorDetails : topFiveDoctors) {

            results.add(DoctorSearchedCountDto.builder()
            .doctor("yes")
                    .name()
            )

        }
        return results;
    }

    static class TopFiveDoctors implements Iterable<DoctorSearchedCountDto> {
        private final Map<Long, Integer> currentDoctors = new HashMap<>();
        private final TreeSet<SongPlayCount> topFive = new TreeSet<>((o1, o2) -> {
            final Long o1Plays = o1.getPlays();
            final Long o2Plays = o2.getPlays();

            final int result = o2Plays.compareTo(o1Plays);
            if (result != 0) {
                return result;
            }
            final Long o1SongId = o1.getSongId();
            final Long o2SongId = o2.getSongId();
            return o1SongId.compareTo(o2SongId);
        });

    private boolean thisHost(final HostStoreDto host) {
        return host.getHost().equals(hostInfo.host()) &&
                host.getPort() == hostInfo.port();
    }

    private HttpHeaders getHttpHeaders() {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.APPLICATION_JSON);
        return httpHeaders;
    }

}
