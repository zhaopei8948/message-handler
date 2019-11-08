package online.zhaopei.messagehandler.controller;

import online.zhaopei.messagehandler.configuration.MessageHandlerProp;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/")
public class ForwardTypeController {

    @Autowired
    private MessageHandlerProp messageHandlerProp;

    @RequestMapping("/modifyForwardType")
    public String modifyForwardType(int type) {
        this.messageHandlerProp.setForwardType(type);
        return "{\"success\": true, \"forwardType\": " + this.messageHandlerProp.getForwardType() + "}";
    }

    @RequestMapping("/getForwardType")
    public String getForwardType() {
        return "{\"success\": true, \"forwardType\": " + this.messageHandlerProp.getForwardType() + "}";
    }
}
