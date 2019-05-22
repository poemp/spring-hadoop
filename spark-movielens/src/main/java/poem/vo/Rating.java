package poem.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Administrator
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Rating {

    private Integer user;

    private Integer movie;

    private Double rating;

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
