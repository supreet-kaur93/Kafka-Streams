package dtos;

import lombok.*;

import java.util.Set;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class HostStoreDto {

    private String host;
    private int port;
    private Set<String> storeNames;

}
