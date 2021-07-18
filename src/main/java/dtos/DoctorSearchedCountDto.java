package dtos;

import lombok.*;


@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
public class DoctorSearchedCountDto {

    private String doctor;
    private String name;
    private Long searched;

}
